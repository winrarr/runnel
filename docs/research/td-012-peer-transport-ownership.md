# TD-012 peer transport ownership

Status: scoped implementation note

Reviewed: 2026-09-06

## Primary/reference findings

- [OpenRaft's `RaftNetworkFactory` documentation](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetworkFactory.html) defines `new_client` as a lazy client constructor for a target node. It does not require the factory to establish a socket, which leaves shared connection ownership to the application network implementation.
- [OpenRaft's `RaftNetwork` documentation](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetwork.html) makes the RPC methods mutable and its default full-snapshot path sends each chunk through repeated `install_snapshot` calls. The current Runnel adapter therefore keeps a serial per-group network stream and does not claim multiplexing.
- [OpenRaft's network implementation guidance](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/index.html#implement-raftnetworkfactory) describes the factory as the owner of network instances for replication targets and reiterates that connection establishment belongs to the later RPC path.

These references support an owner shared by lazy clients without requiring a public protocol change. They do not establish that a shared multiplexed stream is safe or beneficial for Runnel's snapshot and control traffic.

## Current observed behavior

- `GroupManager` constructs one `PeerTransport` and shuts it down from its
  drop boundary. The transport owns only compatibility-pool state; an
  OpenRaft `TcpConnection` still retains its own direct stream for the
  requests that use that client. This is lifecycle isolation, not one shared
  socket for all peer traffic.
- A pooled peer address has at most five compatibility connections: one
  control permit and four shared permits for forwarding and data-group setup.
  The registry retains at most 64 peer addresses. When a full registry has no
  idle pool to evict, a bounded short-lived fallback connection is used
  instead. Idle pooled connections older than 30 seconds are discarded on
  checkout, and failed or timed-out requests do not return their sockets to a
  pool.
- OpenRaft clients are mutable and issue one framed request followed by one
  response on their retained stream. The inbound handler also processes a
  connection serially. While a client has no retained stream, heartbeats and
  votes use the compatibility pool; non-heartbeat append entries and snapshot
  chunks establish/use the retaining client stream, after which that client
  also carries its heartbeats and votes. The reserved compatibility control
  permit therefore does not isolate a snapshot from traffic sharing the same
  persistent OpenRaft client.
- Focused transport tests cover reuse, framing-buffer reuse, pool ownership
  and shutdown, idle expiry, failed/timed-out replacement, bounded fallback,
  concurrent shared traffic, and control reservation. They are in-process
  protocol tests and do not measure end-to-end cluster tail latency.

The observed boundaries come from [`GroupManager`](../../crates/runnel-raft/src/group_manager.rs),
the [outbound peer transport and tests](../../crates/runnel-raft/src/network/outbound.rs),
the [serial inbound handler](../../crates/runnel-raft/src/network/inbound.rs),
and the [opt-in forwarding benchmark](../../scripts/benchmarks/README.md).

## Scoped implementation choice

This slice gives each `GroupManager` one `PeerTransport`. Forwarding, data-group
setup, bounded fallback permits, and compatibility requests from Raft clients
use that owner; each `TcpConnection` still owns any retained direct stream for
its group and target. Dropping the manager's transport drops its
compatibility-pool sockets. The existing per-peer connection cap, control
reservation, idle expiry, timeout behavior, and failed-connection replacement
remain unchanged.

The change deliberately does not pool the persistent per-group Raft streams. That avoids introducing cross-group head-of-line blocking or changing the ordering and failure behavior of OpenRaft's mutable network client. It also does not add a wire version, multiplexing, background reaper, dynamic membership, or snapshot resume semantics.

## Alternatives considered

1. Keep the process-global compatibility pool. This preserves the current behavior but allows unrelated engines or clusters in one process to share idle sockets, capacity, and lifecycle.
2. Pool every Raft request by peer address. This could reduce connection count as group density grows, but would require measured scheduling and head-of-line evidence plus an explicit policy for snapshots, control traffic, ordering, and request cancellation.
3. Add a multiplexed peer protocol. This could isolate logical streams on fewer sockets, but it requires protocol framing/version negotiation, concurrent response routing, bounded per-stream queues, and recovery tests; it is outside this incremental ownership change.
4. Give each compatibility call a short-lived socket. This removes retained idle sockets but makes connection setup part of every forwarded operation and leaves connection churn unmeasured.

## Hypotheses and unresolved risks

- Scoping the compatibility pool to the engine should improve lifecycle isolation and eliminate cross-engine socket reuse; it is not a quantified throughput or latency claim.
- A future peer-address pool may reduce file descriptors when many groups replicate to the same node, but shared sockets can amplify head-of-line blocking and contention unless control and snapshot traffic receive independent bounded capacity.
- The opt-in `peer_forwarding` clustered benchmark now exercises eight concurrent
  follower-ingress publishes against the four shared compatibility permits and
  can inject a bounded delay into `Forward` responses. It reports follower
  round-trip p50/p99/p99.9 and resource samples, but it does not isolate pool
  wait from quorum processing, compare against an alternate connection
  strategy, or exercise snapshot/control interference.
- Snapshots remain serial per OpenRaft network client and can still occupy that
  client's persistent stream. No current evidence establishes whether this
  affects heartbeat latency in the actual OpenRaft scheduler; a focused
  snapshot-plus-control fault/latency benchmark is still needed.
- Pool capacity, fallback behavior, and idle expiry are still fixed policy
  values. Their p99/p99.9 behavior under group density, delayed responses,
  snapshot transfer, and peer replacement remains open, and no authoritative
  transport-strategy performance comparison exists yet.
