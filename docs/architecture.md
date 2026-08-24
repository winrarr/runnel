# Current architecture

Runnel is deliberately split into six crates so the semantic engine contract, local storage, experimental distributed engine, transport, public protocol, and process entry point can evolve independently.

    runnelctl --TCP/JSON lines--> runnel-server --> runnel-engine contract
                                          |              +--> runnel-core (local engine) --> streams/*.log
                                          |              +--> runnel-raft (metadata + per-stream Multi-Raft backend)
                                          +--HTTP--> health and metrics

## Current data model

The public model is currently limited to streams, records, consumers, offsets, and acknowledgements. Publishing creates a stream on first use. Explicit stream creation is also supported. Each stream has a durably replicated internal identity, data-group identity, and lifecycle state in the metadata group. A stream's records and consumer checkpoints live in its own data group. Creation is reconciled through `Creating` and `Active`; requests are served only after the data group is ready and metadata is active. The first clustered layout gives every group the same statically configured three-node membership.

The current record frame stores a magic value, offset, timestamp, optional UTF-8 key, key length, and payload length followed by the key and payload. On startup, the log is scanned. An incomplete trailing frame is truncated to the last complete frame, allowing recovery after a process or machine failure during append.

Clustered Raft logs are compacted independently from retained broker records. After a state-machine snapshot is durable, OpenRaft can purge consensus entries while the snapshot preserves stream records, consumer checkpoints, and producer deduplication state. A replacement node can materialize a missing stream group from replicated metadata when the first group-addressed Raft RPC arrives, then install the validated snapshot before serving recovered state. Snapshot transfer uses bounded chunks, and an interrupted installation can safely restart from byte zero; repeated-interruption cost controls remain operational follow-up work.

Clustered `/metrics` output also exposes aggregate snapshot builds, installs, failures, installed bytes, received chunks, received bytes, final chunks, and installs in progress. These counters describe recovery activity without adding Raft topology to the public engine contract. Peer snapshot chunks are bounded, and an interrupted transfer currently restarts from byte zero rather than persisting partial receiver state.

## Delivery behavior

Polling a non-grouped consumer returns the record at its committed offset. The broker keeps the record in memory as in-flight until it is acknowledged. A second poll before the acknowledgement deadline returns the same record. After the deadline, or after a broker restart, the record is eligible for redelivery. Acknowledgement advances the consumer checkpoint only after the durable state update succeeds.

The local and clustered engines support the same first shared-consumer slice. A named consumer has durable progress, while each grouped poll supplies a transient member name. Members receive different available records, acknowledgements may advance progress out of order, and records with the same key are not delivered concurrently. Each grouped delivery includes an opaque token and attempt number; a token from an expired or reassigned delivery is rejected. The current slice allows one outstanding delivery per member.

The acknowledgement timeout is the initial retry delay. Delivery attempts are persisted with consumer state before a message is returned, so a restart does not reset the retry count. Local delivery and clustered grouped delivery may use a broker-wide maximum attempt count; when a record reaches that limit, the broker copies its key and payload to the stream's `.dead-letter` stream and advances the source consumer past the record. Dead-letter streams are not recursively dead-lettered. The local source checkpoint is persisted after the dead-letter append, preserving at-least-once behavior; a crash between those two durable operations may produce a duplicate dead-letter record. Clustered grouped delivery commits the source progress and dead-letter append as one replicated state transition. The current policy has no backoff or per-consumer override, and does not yet attach origin metadata to the dead-letter payload.

The clustered backend makes consumer progress, in-flight ownership, delivery attempts, lease deadlines, and fencing state part of the per-stream data group's replicated state. Grouped and non-grouped poll and acknowledgement operations are leader-authorized Raft operations and are forwarded when a client connects to another node. Delivery tokens are derived from committed Raft log identities for grouped delivery; the non-grouped compatibility path keeps its tokenless acknowledgement contract. Lease deadlines are absolute timestamps chosen by the leader and carried in the replicated command; this is a first failover baseline, not a final clock or lease-service design. The clustered `--ack-timeout-ms` and `--max-delivery-attempts` settings control expiry and poison-message handling. When the limit is reached, the source progress and a derived `.dead-letter` record commit atomically in the source data group, and the derived stream is addressable through the normal protocol.

This establishes an at-least-once vertical slice for both independent consumers and shared consumers across a static cluster. Acknowledged grouped progress survives replica restart under the data group's Raft durability guarantee, and an expired in-flight delivery may be redelivered after leader or process failure. Clustered dead-letter movement is one replicated state transition; local dead-letter movement remains at least once across separate source and dead-letter records.

## Performance posture

The clustered state machine appends one framed, durable journal record per committed apply entry and replays records after the last durable checkpoint during recovery. Snapshot installation writes a checkpoint and compacts the journal with an atomic replacement, so normal message processing no longer pays for a complete state-file replacement per apply batch. The materialized state and journal are still JSON-based, so this is a correctness-preserving first storage step rather than a final performance architecture.

The current baseline still uses JSON encodings, synchronous atomic file replacement for checkpoints and Raft-log persistence, and a new TCP connection for each internal RPC. Those costs should be measured before replacing them with pooled transport, binary formats, segmented storage, or more aggressive batching. Any optimization must report its durability point and p50/p99/p99.9 behavior, especially for 100-byte and 1-KiB messages.

## Deliberate boundaries

- runnel-core owns persistence and delivery state; it must not depend on a particular network transport.
- runnel-engine owns the topology-free semantic contract shared by local and distributed engines.
- runnel-raft owns the early static Multi-Raft backend, including the metadata group, one data group per stream, versioned local Raft/state-machine files, group-addressed framed TCP peer transport, topology-free client forwarding, replicated publish request deduplication, replicated shared-consumer ownership, clustered retry limits and dead-letter outcomes, consensus-log compaction, and replacement-replica snapshot recovery. It is not yet a complete production cluster: dynamic membership, scalable placement, final lease/fencing policy, backoff and dead-letter provenance, repeated-interruption cost controls, and broader failure semantics remain unfinished.
- runnel-protocol owns the provisional external request/response representation; it must not encode filesystem layout.
- runnel-server owns sockets, HTTP, shutdown, and mapping core errors into protocol responses.
- runnel-cli is a development client and is not a compatibility reference for future language SDKs.

The next architectural changes should preserve these boundaries while replacing the single process-wide lock, introducing segmented storage/indexing, making consumer ownership independently transferable, and adding dynamic membership and recovery without changing application intent.
