# TD-012 peer transport ownership

Status: scoped implementation note

Reviewed: 2026-10-03

Baseline: `origin/main` `49652a19cbd11fe68f79c602df3522a42dfaceba`

Scope: compatibility peer-connection ownership and OpenRaft network-client
lifecycle in the early static clustered backend.

Related: [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
[ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md),
[TD-012](../tech-debt.md#td-012-peer-rpc-connection-strategy-remains-incomplete),
and the [current architecture boundary](../architecture.md).

## Primary/reference findings

- [OpenRaft's `RaftNetworkFactory` documentation](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetworkFactory.html) defines `new_client` as a lazy client constructor for a target node. It does not require the factory to establish a socket, which leaves shared connection ownership to the application network implementation.
- [OpenRaft's `RaftNetwork` documentation](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetwork.html) makes the RPC methods mutable and its default full-snapshot path sends each chunk through repeated `install_snapshot` calls. Runnel's adapter consequently keeps a mutable `TcpConnection` with one retained stream per OpenRaft network client; that client-level serialization is not a multiplexing guarantee for the whole broker.
- [OpenRaft's network implementation guidance](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/index.html#implement-raftnetworkfactory) describes the factory as the owner of network instances for replication targets and reiterates that connection establishment belongs to the later RPC path.
- The pinned OpenRaft 0.9.25 [confirm-leader heartbeat](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/core/raft_core.rs#L334-L355) and [vote](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/core/raft_core.rs#L1068-L1074) paths construct fresh network clients. Starting a replication stream creates separate clients for log replication and snapshot transfer ([stream setup](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/core/raft_core.rs#L815-L827)). The replication loop sends both non-empty append requests and its empty AppendEntries heartbeats through the replication client ([append path](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/replication/mod.rs#L439-L457)); the snapshot task uses only the snapshot client through `full_snapshot` ([snapshot path](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/replication/mod.rs#L780-L797)). Thus replication heartbeats can share a `TcpConnection` with log append requests, but snapshot chunks use a separate client and stream.

These references support an owner shared by lazy clients without requiring a public protocol change. They do not establish that a shared multiplexed stream is safe or beneficial for Runnel's persistent replication streams.

## Current observed behavior

- `GroupManager` constructs one `PeerTransport` shared by all of its groups and
  shuts it down from its drop boundary. The transport owns only
  compatibility-pool state; an OpenRaft `TcpConnection` still retains its own
  direct stream for the requests that use that target/group client. This is
  lifecycle isolation, not one shared socket for all peer traffic.
- A pooled peer address has at most five simultaneous compatibility requests:
  one control permit and four shared permits for forwarding and data-group
  setup. The registry retains at most 64 peer addresses per manager. If the
  registry is full and every entry is still held, the request uses a short-lived
  fallback connection. Fallback capacity is manager-global (one control permit
  and four shared permits), not five additional connections per overflow
  address. Idle pooled connections older than 30 seconds are discarded on
  checkout; failed or timed-out requests do not return their sockets to a pool,
  and there is no background idle reaper.
- `TcpNetwork::new_client` prefers a non-empty address supplied by OpenRaft's
  `BasicNode` and otherwise uses the configured peer map. `TcpConnection` retains
  one framed request/response stream after a non-heartbeat append or snapshot
  RPC. OpenRaft 0.9.25's replication loop uses its retained client for both
  non-empty append requests and empty AppendEntries heartbeats; confirm-leader
  heartbeats and votes use fresh clients and the compatibility pool's reserved
  lane. Snapshot transfer uses a distinct client and stream. The inbound handler
  spawns one task per accepted socket and processes each socket serially. A
  replication RPC can occupy its client's serialized call path, but no current
  evidence shows that moving replication heartbeats to another connection would
  improve heartbeat latency. Snapshot and replication work can also contend for
  the remote process's CPU, Raft state, or storage.
- Peer frames use a big-endian `u32` length prefix, JSON payloads, and a 64 MiB
  body limit. Snapshot chunks are bounded to 64 KiB by the current OpenRaft
  configuration, but the outer protocol has no version preface, capability
  negotiation, authentication, or TLS. Connection ownership is therefore a
  lifecycle and resource boundary, not a peer-trust boundary.
- A request TTL bounds the caller's wait and causes a failed or timed-out local
  connection to be discarded. Once the inbound handler has decoded a request,
  however, the peer may continue processing it until its operation returns; the
  current protocol has no cancellation or request identity for that internal
  work.
- Focused transport tests cover reuse, framing-buffer reuse, pool ownership
  and shutdown, idle expiry, failed/timed-out replacement, bounded fallback,
  concurrent shared traffic, and control reservation. They are in-process
  TCP protocol fixtures rather than tests of `serve_peer` or a multi-process
  cluster, and do not measure end-to-end cluster tail latency.

The observed boundaries come from [`GroupManager`](../../crates/runnel-raft/src/group_manager.rs),
the [outbound peer transport and tests](../../crates/runnel-raft/src/network/outbound.rs),
the [serial inbound handler](../../crates/runnel-raft/src/network/inbound.rs),
and the [opt-in forwarding benchmark](../../scripts/benchmarks/README.md).

## Scoped implementation choice

This slice gives each `GroupManager` one `PeerTransport`. Forwarding, data-group
setup, and stateless control calls use its bounded compatibility pools; each
long-lived OpenRaft replication and snapshot client still owns its direct stream
for its group and target. Dropping the manager's transport drops its idle
compatibility-pool sockets and rejects later requests. The existing per-peer
connection cap, manager-global fallback cap, control reservation, idle expiry,
timeout behavior, and failed-connection replacement remain unchanged.

The change deliberately does not pool the persistent per-group Raft streams. That avoids introducing cross-group head-of-line blocking or changing the ordering and failure behavior of OpenRaft's mutable network client. It also does not add a wire version, multiplexing, background reaper, dynamic membership, or snapshot resume semantics.

## Alternatives considered

1. Keep the process-global compatibility pool. This preserves the current behavior but allows unrelated engines or clusters in one process to share idle sockets, capacity, and lifecycle.
2. Pool every Raft request by peer address. This could reduce connection count as group density grows, but would require measured scheduling and head-of-line evidence plus an explicit policy for snapshots, control traffic, ordering, and request cancellation.
3. Add a multiplexed peer protocol. This could isolate logical streams on fewer sockets, but it requires protocol framing/version negotiation, concurrent response routing, bounded per-stream queues, and recovery tests; it is outside this incremental ownership change.
4. Give each compatibility call a short-lived socket. This removes retained idle sockets but makes connection setup part of every forwarded operation and leaves connection churn unmeasured.

## Hypotheses and unresolved risks

- Scoping the compatibility pool to the engine should improve lifecycle isolation and eliminate cross-engine socket reuse; it is not a quantified throughput or latency claim.
- A future peer-address pool may reduce file descriptors when many groups replicate to the same node, but shared sockets can amplify head-of-line blocking and contention unless control and snapshot traffic receive independent bounded capacity.
- The default opt-in `peer_forwarding` clustered benchmark exercises eight
  concurrent follower-ingress publishes against the four shared compatibility
  permits and can inject a bounded delay into `Forward` responses. Its
  concurrency, delay, timeout, and stream count are configurable. A bounded
  stream-count sweep creates one data group per stream, distributes the fixed
  total measured publishes round-robin, and records the aggregate excluded
  warmup setup. This characterizes follower-ingress publish behavior as data
  group count grows; it does not isolate pool wait from quorum processing,
  attribute connections to individual groups or traffic classes, compare
  against an alternate connection strategy, or exercise snapshot/control
  interference. It now records a settled-boundary per-process socket endpoint
  census; the evidence and limits are recorded below.
- Snapshot chunks are serial on their dedicated OpenRaft snapshot client and do
  not share the transport stream used by that group's log replication and
  replication-loop heartbeats. A real-process snapshot-plus-control probe may
  still be useful to measure shared CPU, Raft-state, or storage contention, but
  transport-level snapshot/control isolation is not justified by the current
  evidence. The replication client's append/heartbeat serialization remains a
  separate hypothesis without measured impact or a demonstrated safe way to
  issue those operations concurrently.
- Pool capacity, fallback behavior, and idle expiry are still fixed policy
  values. Their p99/p99.9 behavior under group density, delayed forwarding
  responses, peer replacement, and overflow-address fairness remains open. The
  forwarding probe does not measure retained per-group replication streams; no
  authoritative transport-strategy performance comparison exists yet.
- Timeout behavior does not prove remote cancellation: a timed-out forwarding
  or Raft request may continue consuming peer and state-machine capacity after
  the caller has abandoned its connection. The interaction between that work,
  retries, duplicate suppression, and unknown outcomes needs explicit failure
  evidence.
- The peer listener currently has no authenticated identity or transport
  encryption. Any future pooling or multiplexing design must preserve the
  current bounded-resource guarantees while defining how a peer is authorized,
  how protocol versions are negotiated, and how a stale connection is fenced.

## Updated disposition

The source review corrects the snapshot/control contention hypothesis but does
not retire TD-012. The current `peer_forwarding` workload can measure forwarding
saturation and follower round-trip latency across bounded data-group counts at
fixed aggregate measured work, and now exposes direct per-node counts of
established broker-owned socket endpoints matching peer ports. Keep the runtime
and ADRs unchanged until a controlled comparison measures a candidate strategy.
Per-group attribution, unique inter-node connection counts,
alternative-strategy comparison, snapshot/control effects, and stable
tail-latency evidence remain open; do not claim an optimization from this
density characterization or the existing pool tests alone.

## Peer socket census evidence

The opt-in scenario takes Linux procfs snapshots after setup warmup and after
measured forwarding. For each broker it joins socket inodes found under
`/proc/<pid>/fd` with established entries in `/proc/<pid>/net/tcp` and `tcp6`,
then counts endpoints whose local port is that broker's peer listener or whose
remote port matches a configured peer destination. Container mode resolves
the broker's host PID with `docker inspect` and first tries the same host-PID
procfs method. Some Docker hosts restrict reading another UID's `/proc/<pid>/fd`;
when that method cannot produce a complete count, a bounded `docker exec` reads
the broker container's `/proc/1/fd` and `/proc/1/net/tcp`/`tcp6`. The fallback
accepts a count only when the Docker-exec UID matches the effective UID of
broker PID 1. It records the observation source and identity check per node. A
missing optional `tcp6` file is allowed; a failed read of an existing TCP table,
missing host PID, identity mismatch, or bounded probe timeout leaves the count
unavailable rather than inferred.

An isolated native-process forwarding smoke on revision
`444652e0b4b4617e058d51cdf2e88ed111f6c23f` observed 8, 4, and 4 endpoints on
nodes 1, 2, and 3 after setup warmup, then 12, 8, and 4 after measured
forwarding. The two census operations took about 17.0 ms and 18.0 ms. The
machine-readable artifact is
`benchmark-results/isolated/25ad03b97ce34cbc/cluster.json`; it covers 20
measured publishes, two streams, two warmup publishes per stream, and
concurrency eight. This is a diagnostic smoke, not an authoritative performance
comparison.

An initial isolated container attempt on baseline revision
`8511b09d7d6951d8a2281e04027f71365ad7202d` found Docker host PIDs but could not
read their socket ownership through host procfs; all node counts were
unavailable in `benchmark-results/isolated/823306bfcec4450b/cluster-container.json`.
This diagnostic exposed the host's procfs restriction and did not substitute
configured stream counts or request metrics.

After adding the bounded container-procfs fallback, an isolated three-node
container forwarding smoke on revision
`fc33ff18a8270112138b609964e47b30fae4c259` observed 8, 4, and 4 established
socket endpoints on nodes 1, 2, and 3 after setup warmup, then 12, 8, and 4
after measured forwarding. Each node used
`docker_exec_container_procfs_fd_inode_join`; `docker_host_pid_resolved` and
`container_process_identity_verified` were true for all six samples. The
machine-readable result is
`benchmark-results/isolated/952d70ec76574f21/cluster-container.json`. It covers
20 measured publishes, two streams, two warmup publishes per stream, and
concurrency eight. The sequential census operations took 208.9 ms after warmup
and 218.0 ms after measured forwarding. The isolated workflow validated both
boundaries and all three available non-negative per-node counts, then removed
its containers, network, image, and unique temporary directory. This is a
diagnostic smoke, not an authoritative performance comparison.

These are per-process socket endpoint counts, not unique connections: one
inter-node connection can be represented at both brokers. Port matching does
not identify the Raft group or distinguish replication, snapshot, compatibility
pool, or other peer-port traffic. The snapshots are read sequentially and are
not an atomic cluster-wide sample; collection time is outside the forwarding
measurement. The real-container census gap is now covered for this supported
Linux+Docker smoke path, but one diagnostic run does not establish repeatability
across hosts or runtimes. Direct endpoint observability is available where one
of the two recorded procfs paths succeeds, while the broader transport strategy
question remains unresolved.
