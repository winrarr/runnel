# TD-012 peer transport ownership

Status: scoped implementation note

Reviewed: 2026-09-02

## Primary/reference findings

- [OpenRaft's `RaftNetworkFactory` documentation](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetworkFactory.html) defines `new_client` as a lazy client constructor for a target node. It does not require the factory to establish a socket, which leaves shared connection ownership to the application network implementation.
- [OpenRaft's `RaftNetwork` documentation](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetwork.html) makes the RPC methods mutable and its default full-snapshot path sends each chunk through repeated `install_snapshot` calls. The current Runnel adapter therefore keeps a serial per-group network stream and does not claim multiplexing.
- [OpenRaft's network implementation guidance](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/index.html#implement-raftnetworkfactory) describes the factory as the owner of network instances for replication targets and reiterates that connection establishment belongs to the later RPC path.

These references support an owner shared by lazy clients without requiring a public protocol change. They do not establish that a shared multiplexed stream is safe or beneficial for Runnel's snapshot and control traffic.

## Scoped implementation choice

This slice gives each `GroupManager` one `PeerTransport`. All of its Raft network clients, forwarding requests, data-group setup requests, and bounded fallback permits use that owner. Dropping the manager's transport drops its compatibility-pool sockets. The existing per-peer connection cap, control reservation, idle expiry, timeout behavior, and failed-connection replacement remain unchanged.

The change deliberately does not pool the persistent per-group Raft streams. That avoids introducing cross-group head-of-line blocking or changing the ordering and failure behavior of OpenRaft's mutable network client. It also does not add a wire version, multiplexing, background reaper, dynamic membership, or snapshot resume semantics.

## Alternatives considered

1. Keep the process-global compatibility pool. This preserves the current behavior but allows unrelated engines or clusters in one process to share idle sockets, capacity, and lifecycle.
2. Pool every Raft request by peer address. This could reduce connection count as group density grows, but would require measured scheduling and head-of-line evidence plus an explicit policy for snapshots, control traffic, ordering, and request cancellation.
3. Add a multiplexed peer protocol. This could isolate logical streams on fewer sockets, but it requires protocol framing/version negotiation, concurrent response routing, bounded per-stream queues, and recovery tests; it is outside this incremental ownership change.
4. Give each compatibility call a short-lived socket. This removes retained idle sockets but makes connection setup part of every forwarded operation and leaves connection churn unmeasured.

## Hypotheses and unresolved risks

- Scoping the compatibility pool to the engine should improve lifecycle isolation and eliminate cross-engine socket reuse; it is not a quantified throughput or latency claim.
- A future peer-address pool may reduce file descriptors when many groups replicate to the same node, but shared sockets can amplify head-of-line blocking and contention unless control and snapshot traffic receive independent bounded capacity.
- Snapshots remain serial per OpenRaft network client and can still occupy that client's persistent stream. No current evidence establishes whether this affects heartbeat latency in the actual OpenRaft scheduler; a focused fault/latency benchmark is still needed.
- Pool capacity, fallback behavior, and idle expiry are still fixed policy values. Their p99/p99.9 behavior under group density, delayed responses, snapshot transfer, and peer replacement remains open.
