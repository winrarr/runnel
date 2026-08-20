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

Polling a consumer returns the record at its committed offset. The broker keeps the record in memory as in-flight until it is acknowledged. A second poll before the acknowledgement deadline returns the same record. After the deadline, or after a broker restart, the record is eligible for redelivery. Acknowledgement advances the consumer checkpoint only after the checkpoint file has been atomically replaced and synced.

This establishes an at-least-once vertical slice. It does not yet implement consumer groups, concurrent ownership, batching, or a distributed coordination protocol.

## Performance posture

The clustered state machine persists one durable state update per committed apply batch and snapshots copy the state before serialization, so a batch does not pay one state-file replacement per entry and snapshot encoding does not hold the state read lock. These are correctness-preserving first wins, not a performance claim.

The current baseline still uses JSON encodings, synchronous atomic file replacement, and a new TCP connection for each internal RPC. Those costs should be measured before replacing them with pooled transport, binary formats, segmented storage, or more aggressive batching. Any optimization must report its durability point and p50/p99/p99.9 behavior, especially for 100-byte and 1-KiB messages.

## Deliberate boundaries

- runnel-core owns persistence and delivery state; it must not depend on a particular network transport.
- runnel-engine owns the topology-free semantic contract shared by local and distributed engines.
- runnel-raft owns the early static Multi-Raft backend, including the metadata group, one data group per stream, versioned local Raft/state-machine files, group-addressed framed TCP peer transport, topology-free client forwarding, replicated publish request deduplication, consensus-log compaction, and replacement-replica snapshot recovery. It is not yet a complete production cluster: dynamic membership, placement, fencing policy, repeated-interruption cost controls, and broader failure semantics remain unfinished.
- runnel-protocol owns the provisional external request/response representation; it must not encode filesystem layout.
- runnel-server owns sockets, HTTP, shutdown, and mapping core errors into protocol responses.
- runnel-cli is a development client and is not a compatibility reference for future language SDKs.

The next architectural changes should preserve these boundaries while replacing the single process-wide lock, introducing segmented storage/indexing, making consumer ownership independently transferable, and adding dynamic membership and recovery without changing application intent.
