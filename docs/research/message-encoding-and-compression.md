# Message encoding and compression study

- Status: research-backed study; the local stream format is accepted separately by ADR 0039
- Last reviewed: 2026-10-05
- Baseline inspected: `db86fd2793f4904712a11314a21e149abdcf1897`
- Evidence class: research/design
- Scope: public request/response payloads, retained message records, and the
  clustered peer transport
- Current evidence: [ADR 0039](../decisions/0039-rnl1-write-admission-and-legacy-read-compatibility.md), [protocol compatibility design](../design/protocol-compatibility.md),
  [storage compatibility evidence](../design/td-007-storage-compatibility-evidence.md),
  [clustered outcome contract](../design/clustered-outcome-contract.md), and
  [distributed architecture exploration](distributed-architecture-options.md)
- Accepted boundaries: [ADR 0022](../decisions/0022-provisional-binary-payloads.md)
  keeps binary-safe payloads additive in the provisional JSON protocol;
  [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)
  accepts segmented retained data and independent placement identity while
  deferring the exact segment and encoding formats.
- Related research: [Systems performance research for Runnel](systems-performance-research.md)
  adds current code observations about repeated batch serialization and
  clustered payload copies; treat those as hypotheses to measure alongside
  the encoding and allocation matrix below.

This document records the evidence and hypotheses behind the backlog outcome
[Make message encoding and compression evolvable](../backlog.md#make-message-encoding-and-compression-evolvable).
It does not change the public wire or codec. ADR 0039 already selects the
local RNL3 v2 format and refuses obsolete local frames; future schema and
compression choices still require focused design, tests, and measurements.

## Decision summary

The current system already has a useful binary-safety slice, but not an
evolvable encoding/compression contract:

- The public path is one UTF-8 JSON object per TCP line. Text payloads use the
  legacy `payload` string; arbitrary payload bytes use the explicit padded
  base64 `PublishBytes`, `PublishBatch`, `MessageBytes`, and replay variants.
  Publish batches are bounded and return ordered per-record outcomes; they do
  not imply atomicity.
- Local retained records use one checksummed RNL3 version-2 frame, including
  ordinary records without an identity. Its typed identity flag and bounded
  key, payload, and identity fields are part of the accepted local format.
  RNL1, RNL2, and RNL3 version 1 are refused before recovery mutation.
- Cluster peer RPCs remain a custom big-endian `u32` length prefix around JSON,
  capped at 64 MiB, with no protocol preface or codec negotiation. Snapshot
  transfer is configured as 64 KiB chunks over that outer frame. A peer frame
  can carry Raft control RPCs, forwarded operations, or a snapshot chunk, so
  one universal message-record format would couple unrelated compatibility
  lifecycles.
- Compression is not implemented on the public, retained-record, or peer
  paths. The Raft log, state-machine journal, checkpoint, and snapshot formats
  are separate JSON persistence boundaries. The journal is synced before
  committed state-machine application, while log/checkpoint/snapshot
  replacement has its own atomic-write rules; none should silently inherit a
  retained-message codec decision.
- The accepted [binary-payload decision](../decisions/0022-provisional-binary-payloads.md)
  preserves the additive base64 JSON path without choosing a binary schema;
  [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)
  accepts a future segmented retained-data boundary without choosing that
  segment's schema, compression, or whether its bytes are shared with Raft.

The accepted local storage boundary is RNL3 version 2 for all records,
as selected by ADR 0039. Further compression or encoding work must name
the transformed boundary, use explicit frame/version semantics, preserve
current durability and message behavior, and establish bounded recovery.
No old local reader or mixed-format recovery path is a requirement.

## How to read this document

The labels below keep observed behavior separate from design intent:

- **Observed:** behavior inspected in the current Rust code and tests.
- **Sourced fact:** behavior documented by an external standard, project, or
  primary research source linked directly.
- **Inference:** a deduction about Runnel from observed behavior and sourced
  facts; it is not a claim made by the cited source.
- **Hypothesis/proposal:** a candidate for future implementation, not a
  compatibility promise.
- **Acceptance evidence:** tests or measurements that would be required before
  an ADR or default change.

## Current observed boundary

The source of truth for current behavior is Rust code and tests, not this
proposal. The relevant boundaries are the [provisional protocol types](../../crates/runnel-protocol/src/lib.rs),
[server framing and response serialization](../../crates/runnel-server/src/protocol.rs),
[request dispatch](../../crates/runnel-server/src/dispatch.rs),
[local stream log](../../crates/runnel-core/src/stream_log.rs),
[peer frame codec](../../crates/runnel-raft/src/network/framing.rs), and
[state-machine command model](../../crates/runnel-raft/src/state_machine.rs),
[Raft log store](../../crates/runnel-raft/src/log_store.rs),
[state-machine journal](../../crates/runnel-raft/src/state_machine_journal.rs),
and [state-machine snapshots and checkpoints](../../crates/runnel-raft/src/state_machine_store.rs).

| Boundary | Observed current behavior | What is still not established |
|---|---|---|
| Public client protocol | Serde-tagged JSON requests and responses are exchanged as one line per request. Incoming request bytes must be UTF-8. The protocol crate, reusable client, and server expose the same source-level `runnel-json-lines` v1 support declaration, but the listener does not advertise or negotiate it at runtime. The configured request-frame limit is bounded above by 64 MiB and includes the JSON/base64 representation, not just decoded payload bytes. The client separately bounds response buffering with its local `max_response_bytes` setting; the server does not negotiate or enforce that client-side value. | No public binary protocol, runtime version negotiation, compatibility range, or stable wire schema exists. A base64 request can consume substantially more wire space than its logical payload, and v1 does not expose an authoritative operation stage/outcome on errors. |
| Public payloads | `Publish` accepts a UTF-8 `String`. `PublishBytes` and `PublishBatch` carry `BinaryPayload`, which is standard padded base64 in JSON and decodes to `Vec<u8>`. Responses choose the readable UTF-8 variant or an explicit base64 variant without changing logical bytes. Publish batches are capped at 1,024 records and preserve input order with one outcome per record; a batch is not atomic. | The current JSON path is a development representation, not a compact binary contract. The optional ordering key remains an application-visible UTF-8 string; changing key semantics to arbitrary bytes would be a separate decision. A future envelope must preserve per-record outcomes and the distinction between confirmed, rejected, retryable, and unknown work. |
| Local stream records | Every record uses the checksummed RNL3 version-2 frame. The 48-byte header carries the version, typed identity flag, offsets, timestamp, bounded lengths, and CRC-32C; keys are capped at 128 bytes, payloads at 64 MiB, and identity strings at 1 KiB. | Compression and codec negotiation are not implemented. RNL1, RNL2, and RNL3 version 1 are refused before incomplete-tail repair; no backward-read promise exists. |
| Local recovery | Startup scans every stream using the RNL3 v2 reader before truncating any incomplete final suffix. Complete malformed records, unsupported versions/flags, offset gaps, and checksum failures fail recovery. | Per-record bounds do not establish aggregate memory use for retained identities, payload reads, responses, or concurrent operations; see [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget). |
| Peer transport | Peer requests and responses use a persistent or pooled TCP connection with a big-endian `u32` body length and JSON body. The outer frame cap is 64 MiB, and the OpenRaft snapshot policy limits individual chunks to 64 KiB. `PeerRequest` covers Raft control RPCs, forwarding, and data-group setup; snapshot chunks travel through the same outer framing. `serde_json` serializes command `Vec<u8>` values as JSON integer arrays. | There is no connection preface, version/capability handshake, codec negotiation, or rule preventing a new writer from sending a body an older peer cannot interpret. Inbound code checks the declared JSON length before resizing its frame buffer; outbound code materializes the serialized frame before checking the 64 MiB cap, so the cap is not a pre-serialization allocation bound. Snapshot chunking is not resumable format migration. |
| Clustered persistence | The Raft log, state-machine journal, checkpoints, and snapshots have separate JSON formats and version/recovery rules. `Command::Publish` carries payload as `Vec<u8>`, which JSON encodes as integer arrays in Raft entries and journal records. The journal uses a little-endian `u32` length plus JSON, caps each record at 64 MiB, reads the journal file during recovery, truncates a partial final record, and is synced before state-machine application. The Raft log, checkpoints, and snapshots use their own atomic replacement paths; snapshots still materialize complete retained state. | Compressing or changing the retained-message frame alone cannot reduce these separately serialized Raft, journal, checkpoint, snapshot, or peer representations. Any candidate must name which file/network boundary it transforms and separately bound its encoded and decoded size. Each artifact needs its own version, failure, recovery, and migration gate. |

The local engine preserves the important semantic boundary: payloads are
`Vec<u8>` internally, offsets are logical record positions, and consumer
acknowledgement state is persisted independently of the physical payload
bytes. Its storage executor admits bounded per-stream work, but that execution
bound is not a wire or durable-format boundary. A compressed block therefore
cannot become an acknowledgement unit or change the ordering and redelivery
model. The clustered engine similarly keeps retained records, consumer state,
and deduplication inside per-stream replicated state while consensus and peer
frames remain separate concerns.

## Constraints for an evolvable design

These constraints apply to future proposals; they do not imply that the
current implementation already satisfies them.

1. **Byte preservation:** an opaque payload must round-trip byte-for-byte. The
   broker must not parse, transcode, normalize, or infer an application
   schema. UTF-8 validation applies to fields that are defined as text, not to
   payload bytes.
2. **Semantic preservation:** offsets, published timestamps, optional UTF-8
   ordering keys, at-least-once delivery, acknowledgement ordering, retry
   attempts, request identities, dead-letter identity, and ordered per-record
   batch outcomes must retain their current logical meaning. Encoding must not
   turn a partial or ambiguous batch result into an implicit all-or-nothing
   claim.
3. **Independent boundaries:** logical payload, schema-encoded envelope, wire
   frame, and durable frame are separate concepts. A transport codec must not
   silently become an at-rest codec, and a storage block must not become a
   delivery or acknowledgement batch.
4. **Bounded work before allocation:** a reader must validate magic/version,
   header length, flags, stored length, decoded length, record count, key and
   request-ID lengths, compression window, dictionary identity, and configured
   maxima before allocating or decompressing. Integer overflow and expansion
   beyond the local limit must fail closed.
5. **Observable corruption:** an incomplete final write may be recoverable as a
   torn suffix when the format can prove that it is incomplete. A complete
   frame with a bad checksum, impossible length, unknown required feature, or
   invalid text field must not be silently skipped.
6. **Format changes:** Runnel has no backward-compatibility requirement.
   A new local format may replace the current one after an explicit
   decision and focused recovery evidence; do not retain readers only
   to keep obsolete local artifacts readable.
7. **Client reach:** a future binary schema must have maintained
   implementations for the languages Runnel intends to support. A generated
   schema dependency is acceptable only if its toolchain, field policy, and
   debugging path are explicit.
8. **Resource evidence:** compression ratio alone is insufficient. Encoding
   and codec CPU, allocations, memory, batch wait, storage bytes, network
   bytes, recovery time, and p50/p99/p99.9 latency must be measured under the
   target resource budget.

## Encoding alternatives

The candidates below are evidence about possible schema encodings, not choices
made by Runnel. Every candidate still needs a Runnel-owned outer frame for
message boundaries, limits, checksum coverage, and storage recovery.

| Candidate and primary source | Relevant sourced behavior | Runnel-specific difference and fit |
|---|---|---|
| [Protocol Buffers encoding guide](https://protobuf.dev/programming-guides/encoding/) and [proto3 language guide](https://protobuf.dev/programming-guides/proto3/) | The binary wire uses numbered fields and length-delimited `bytes`; the decoder needs the schema to interpret field numbers. Proto3 binary parsing preserves unknown fields, while JSON conversion can lose them. Field order is not guaranteed to be stable. | Strong leading candidate for typed public requests/responses and peer commands because opaque payloads map directly to `bytes` and additive fields have explicit schema rules. It requires `.proto` ownership, code generation, reserved field numbers, and a policy for old peers that do not understand a new semantic field. It is not a canonical byte-string or a self-describing durable record by itself. |
| [CBOR, RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html) | CBOR adds byte strings to a JSON-like data model and explicitly targets extensibility without requiring version negotiation. Multiple valid encodings can represent the same data; maps have no semantic key order unless a deterministic profile says otherwise. | Good compatibility and tooling bridge for dynamic clients and binary-safe development traffic. Runnel would need a schema/profile, deterministic-encoding rule if bytes are hashed or signed, map/array limits, and strict duplicate/unknown-field handling. Its flexibility shifts more validation into Runnel and does not replace an outer durable frame. |
| [MessagePack specification](https://github.com/msgpack/msgpack/blob/master/spec.md) | The format has distinct `str` and `bin` families, arrays, maps, and application-defined extension types. The specification describes a type system and formats, while profiles and schema/determinism rules are left to applications. | Easy to prototype beside Serde and useful as a JSON-like comparator. Binary payloads are direct, but struct-as-map versus struct-as-array and extension semantics would become a Runnel profile. The specification alone does not supply the compatibility policy needed for long-lived retained records or peer commands. |
| [FlatBuffers evolution](https://flatbuffers.dev/evolution/) and [FlatBuffers internals](https://flatbuffers.dev/internals/) | Tables use offsets/vtables; old code can ignore newly added fields when schema-evolution rules are followed. The format is designed for access without first unpacking a full object, with alignment and verifier considerations. | Worth testing only if peer hot paths or large replay reads show decode/copy cost as material. Generated code, verifier limits, alignment, and builder order increase the first-slice surface. A FlatBuffer still needs Runnel framing, offset/index rules, and corruption handling for a durable log. |
| [Cap'n Proto schema language](https://capnproto.org/language.html) and [encoding specification](https://capnproto.org/encoding.html) | Cap'n Proto is strongly typed and not self-describing. Field ordinals support compatible additions; the native representation uses pointers/segments, and optional packing can reduce transmission size. It also defines a canonicalization path separately from ordinary encoding. | Attractive for a zero-copy peer experiment, but the schema/compiler and traversal limits are a larger client and storage commitment. Its pointer/segment representation does not define Runnel's record offsets, block checksums, or migration selector. The peer protocol would still need a Runnel-owned handshake and outer bounds. |

### Encoding inference and preliminary direction

The strongest preliminary direction is Protocol Buffers for a future typed
public/peer envelope, with CBOR retained as the dynamic-tooling fallback. This
is an inference from the direct `bytes` field, explicit field-number evolution,
and expected language-client needs; it is not an accepted Runnel choice. The
first implementation should not add a generated schema dependency merely to
test framing and compression. MessagePack is a reasonable prototype
comparator. FlatBuffers and Cap'n Proto should be deferred until a measured
zero-copy requirement exists.

Whichever schema is selected, do not persist or transmit an unbounded generic
object. Define a small Runnel message envelope with explicit metadata and one
opaque payload field. Keep schema versions, frame versions, and compression
identifiers independently visible so a reader never has to infer one from the
other.

## Compression alternatives and scope

### Sourced facts

- Apache Kafka documents producer compression over full batches, with
  `none`, `gzip`, `snappy`, `lz4`, and `zstd` choices. Its record-batch format
  carries compression attributes and a CRC-32C over the batch body: [producer
  configuration](https://kafka.apache.org/43/configuration/producer-configs/),
  [record-batch format](https://kafka.apache.org/43/implementation/message-format/),
  and [end-to-end batch compression](https://kafka.apache.org/43/design/design/).
  The documented producer path lets the broker validate the batch, retain it
  compressed in the log, and send compressed batches to consumers. The broker
  `compression.type` setting can retain the producer codec or force a different
  final codec: [broker configuration](https://kafka.apache.org/43/configuration/broker-configs/).
- Apache Pulsar's binary protocol places the compression algorithm and
  original uncompressed size in message metadata and compresses a complete
  batch as a unit. This specifies a producer-to-broker wire representation;
  it does not select Runnel's retained-record format: [Pulsar 4.2 binary
  protocol](https://pulsar.apache.org/docs/4.2.x/developing-binary-protocol/).
- Redpanda documents the same producer-side full-batch shape and says producer
  compression is retained/served as-is, while noting that compression costs
  CPU: [producer guidance](https://docs.redpanda.com/streaming/current/develop/produce-data/configure-producers/)
  and [topic properties](https://docs.redpanda.com/streaming/current/reference/properties/topic-properties/).
- The official [LZ4 frame specification](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md)
  supports bounded block sizes, optional block/content checksums, dictionary
  IDs, and linked or independent blocks. Linked blocks can improve ratio but
  require sequential history, which limits random access and parallel decode.
- [RFC 8878](https://www.rfc-editor.org/rfc/rfc8878.html) defines Zstandard
  frames with optional content size, checksum, dictionary ID, and a window
  that bounds the decoder's history requirement. Dictionaries are identified
  but supplied out of band. [RFC 9659](https://www.rfc-editor.org/rfc/rfc9659.html)
  sets an 8 MiB window limit for HTTP `zstd` content coding; that rule is not
  a limit for a Runnel frame unless Runnel adopts it explicitly.
- NATS separates its byte-counted client payload from application data
  formats: [client protocol](https://docs.nats.io/reference/protocols/client)
  and [message structure](https://docs.nats.io/using-nats/developer/sending/structure).
  JetStream's `Compression` setting is an at-rest file-store setting (`s2` or
  none), not evidence of a compressed client payload contract: [stream
  configuration](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md).
- Zstandard's original project comparison defines size ratio, encode speed,
  and decode speed as separate measures, notes that data type changes the
  outcome, and says the useful level depends on the workload and hardware:
  [Meta Engineering's Zstandard overview](https://engineering.fb.com/2016/08/31/core-infra/smaller-and-faster-data-compression-with-zstandard/).
  Its 2016 corpus and machine results are not a Runnel performance estimate.

| Reference design | Compression boundary | What its primary documentation establishes | Difference that matters to Runnel |
|---|---|---|---|
| [Kafka record batches](https://kafka.apache.org/43/design/design/) | Producer-compressed batch can be validated by the broker, retained compressed in the log, and delivered compressed; broker policy can preserve or choose the final codec. | Codec is in batch attributes and CRC-32C covers the record-batch body: [format](https://kafka.apache.org/43/implementation/message-format/) and [broker `compression.type`](https://kafka.apache.org/43/configuration/broker-configs/). | A single batch representation can serve ingress, storage, and fetch, but Kafka's batch identity, validation, and offset rules are already designed around that format. Runnel's per-record outcomes and separately persisted state need their own semantics and failure boundaries. |
| [Pulsar producer batches](https://pulsar.apache.org/docs/4.2.x/developing-binary-protocol/) | Producer marks compressed payload metadata and compresses the complete batch before sending it. | The protocol exposes the compression algorithm and original uncompressed payload size; each batched message retains its own metadata and payload size. | Demonstrates explicit decode-size metadata and batch compression. It does not establish a Runnel at-rest or Raft-log policy; those are separate paths in this repository. |
| [JetStream file store](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md) | File-backed stream storage can use `s2` compression. | Compression is configured as a storage property; the docs do not describe this as client-wire compression. | Demonstrates that at-rest compression can be a storage-local choice independent of the client payload contract. Runnel's local stream log and clustered persistence would still need separate formats and recovery rules. |

These systems demonstrate why compression scope matters, but their policies
are not Runnel specifications. In particular, Kafka's record batches and
NATS's file-store compression do not establish equivalent acknowledgement,
replay, or peer-recovery semantics for Runnel.

### Candidate comparison

| Candidate | Useful property | Runnel cost/risk to measure |
|---|---|---|
| None | No codec CPU, no expansion risk, simplest recovery. It is an explicit baseline, not absence of policy. | More storage and wire bytes; JSON/base64 overhead remains on the provisional public path. |
| LZ4, independent blocks | Low-CPU candidate with bounded blocks and a natural option for random block access. Block/content checksums can supplement Runnel's outer checksum. | Ratio may be insufficient for small or already-compressed payloads. Independent blocks may lose cross-record redundancy; linked blocks complicate random replay and parallel decode. |
| Zstandard, low level and bounded window | Higher ratio candidate with a standardized frame, explicit window/content-size metadata, and fast decompression. | Encoder CPU, window memory, decode expansion, and level-dependent tail latency. Dictionary IDs require distribution, versioning, and retirement; an absent dictionary must be a deterministic failure, not an ambient fallback. |
| gzip or Snappy | Kafka/Pulsar make them useful codec comparators; JetStream documents the separate `s2` at-rest choice. | No current Runnel requirement calls for them. Adding more codecs expands capability negotiation, test matrices, security review, and maintenance. Include only for a measured workload or external interoperability need. |

### Candidate placement across Runnel boundaries

Compression location is a separate decision from the codec. The references
show three useful shapes: Kafka validates producer-compressed batches and can
keep the same batch compressed through its log and consumer fetch path; Pulsar
marks client-compressed payloads with codec and original-size metadata and
compresses a whole batch; JetStream's `s2` option applies to file-backed stream
storage. These are different lifecycle choices, not interchangeable codec
defaults.

| Candidate boundary | What it can reduce | Costs and Runnel-specific constraints |
|---|---|---|
| Application payload before publish | Bytes the client sends, and potentially retained/replicated payload bytes if the compressed bytes become the broker's opaque message. Compression work stays with the producer. | Consumers must decompress the published bytes themselves; preserving a transparent original-payload API would require encoding metadata and broker-side decode. The current clustered JSON integer-array envelope may erase payload savings, so measure encoded frames too. Broker consumers, keys, request IDs, and deduplication must not infer the application codec. Per-message compression has little shared history and can grow tiny or already-compressed payloads. |
| Client publish batch before public transport | Public wire bytes and producer calls; batching can expose repeated metadata or payload patterns. Kafka and Pulsar show producer-side whole-batch compression. | The server must bound both compressed and decoded sizes before allocation. Runnel returns ordered per-record outcomes and does not make a batch atomic, so one compressed client batch cannot silently become one all-or-nothing publish or acknowledgement. Broker decoding followed by re-encoding may erase wire savings and add a copy. |
| Public response or connection frame | Bytes on a slow or expensive client link, including JSON field and base64 expansion if compression wraps the complete encoded response. It can be negotiated without changing durable bytes. | It saves neither local storage nor Raft/journal bytes. It adds CPU and buffering per connection, can increase latency for small responses, and needs explicit negotiated codec, compressed-frame and decoded-frame limits. Current JSON remains UTF-8 and has no such handshake. |
| Peer RPC frame | Network bytes for forwarding, Raft entries, or snapshot transfer. Compressing each outgoing frame is independent of the local durable message format. | It does not reduce local Raft-log or state-machine journal bytes. Per-peer recompression multiplies leader CPU; compressed inbound data still needs bounded decoding. Control RPCs and small frames may cost more than they save. Negotiation, reconnect, unsupported-peer behavior, and snapshot framing need a peer-specific gate. |
| Local retained-message record/block | Bytes appended and later read from the stream log. A bounded independent block can amortize headers and use redundancy across records while keeping a local storage choice. | Compression CPU runs before local publish durability completes; replay and recovery pay decode cost. A larger block can raise batch wait, peak memory, read amplification, and p99 for one-record replay. Offset lookup, checksum coverage, torn-tail recovery, and per-record acknowledgements remain separate from the physical block. |
| Raft log, state-machine journal, checkpoint, or snapshot | Only the selected replicated persistence artifact. Compressing a Raft entry may reduce consensus log and peer bytes; compressing a journal may reduce state-machine journal writes/recovery bytes; compressing a snapshot may reduce snapshot storage/transfer. | These are separate versioned artifacts. Applying a compressed Raft command still requires logical records in state; compressing the retained log does not compress state-machine snapshots. Each artifact needs its own reader, checksum, bounds, failure mode, and upgrade plan. The current state journal and snapshot encode `Vec<u8>` as JSON integer arrays, so a binary schema change may remove representation overhead without codec CPU. |

**Runnel inference:** first measure where bytes and CPU are actually spent, then
place compression only on a path whose network or storage traffic is a
material constraint. Treat public wire, peer RPCs, local retained blocks, and
Raft/state-machine artifacts as separate experiments. For each candidate,
compare a direct binary envelope with compressed JSON as separate rows: the
current peer `Vec<u8>` representation is a JSON integer array, and compression
can make that verbose representation smaller while still paying serialization,
compression, and decompression work. Do not call compression a win solely
because compressed payload length falls. Decline it when total framed bytes do
not fall enough to offset codec work, or when p99/p99.9 latency, peak memory,
recovery time, or resource headroom worsens beyond the workload's stated
budget. Set no size threshold until those measurements exist.

### Compression scope inference

Per-record compression keeps random access and corruption boundaries simple,
but repeats frame/window overhead and gives 100-byte messages little shared
history. A bounded batch/block amortizes codec calls and headers and can use
the correlation between adjacent records, but replay may decompress unrelated
records and consume more memory. Linked blocks improve history reuse at the
cost of sequential recovery and parallelism.

The likely Runnel shape is therefore a bounded physical block containing
multiple independently indexed logical records, with an explicit first/last
offset or offset index. A block is only a storage/transport unit: each record
keeps its own offset, delivery, and acknowledgement identity. This is an
inference from the Kafka and LZ4 boundaries, not a performance claim.

Start with independent blocks and no shared dictionary. If measurements later
justify linked blocks or dictionaries, the block format must carry the needed
history/ID metadata and prove bounded replay, portability, rolling-upgrade,
and cleanup behavior first.

## Hypothetical format and negotiation boundary

The following is a proposal to test, not an accepted byte layout.

### Four layers

1. **Logical message:** stream identity, optional UTF-8 ordering key, broker
   timestamp/offset, and opaque payload bytes.
2. **Schema-encoded envelope:** a small typed representation of metadata and
   payload. The schema codec is identified independently from compression.
3. **Runnel-owned frame:** magic, format version, flags, lengths, record/block
   metadata, encoding/compression/dictionary IDs, and corruption checksum.
4. **Transport or storage carrier:** connection frame, durable segment, or
   snapshot chunk with its own lifecycle and compatibility policy.

Do not use the schema library's serialization defaults as the storage contract.
The outer frame must make these values unambiguous:

- frame/header version and bounded header length;
- stored body length and the precise pre-compression encoded-body length;
- logical record count and offset range or an index sufficient for offset lookup;
- key/request-ID lengths where those fields are outside the body;
- encoding, compression, and dictionary identifiers;
- checksum algorithm, coverage, and the checksum field's canonical zeroing
  rule; and
- reserved bits/fields, maximums, and failure behavior for unknown values.

Do not overload `logical_len` to mean both decoded envelope bytes and total
payload bytes. If both are needed, carry both or define one as derived with an
explicit overflow-safe rule. The frame checksum should cover the decoding
metadata, key/request identity, and stored bytes after the checksum field is
zeroed. This detects accidental corruption of compressed bytes and routing
metadata; it is not authentication. A separate digest would be a later
security decision.

### Public and peer wire evolution

The current JSON-lines protocol should remain the debuggable v1 path while a
binary path is experimental. A future binary connection needs an unambiguous
preface followed by a `Hello` exchange that advertises and selects:

- protocol major/minor versions;
- schema encodings and compression algorithms;
- maximum stored frame, decoded envelope, decompressed block, and batch sizes;
- supported dictionary IDs, if dictionaries ever exist; and
- optional operations such as publish batches or binary payload responses.

The selected combination is connection-scoped. A reconnect repeats the
handshake. A malformed preface must not be guessed as JSON, and a connection
must not silently downgrade after application data has been exchanged. A
legacy JSON client remains explicit and does not become a base64-only contract
by silently changing the meaning of its `payload` field.

Peer control RPCs, forwarded message operations, and snapshot chunks should
have separate schema/version gates even if they share a bounded outer frame.
Control traffic may reasonably stay uncompressed until evidence says
otherwise. A peer may send compressed data only after the receiver has
advertised the codec and limits; a wire choice must not force a replica to
persist bytes it cannot recover. The Raft/state-machine journal and snapshot
formats remain independently versioned.

### Durable records and current-format changes

RNL3 version 2 is the only current local stream format. It stores ordinary
records and typed public/dead-letter identities in one checksummed frame
family. RNL1, RNL2, and RNL3 version 1 are rejected before recovery can
truncate an incomplete suffix. There is no legacy reader, writer selector,
or audit/export route to maintain.

A future local codec change should use an explicit frame version or family
and define field limits, checksum coverage, bounded decoding, crash-tail
recovery, and non-mutating refusal behavior. It must preserve current
logical offsets, request-ID semantics, and acknowledgement ordering. This
does not require old local frames to remain readable. Segment layout,
retention, and local-to-cluster migration have separate design records:
[storage upgrade policy](../design/storage-upgrade-policy.md) and
[safety plan](../design/storage-upgrade-safety-plan.md).

## Research hypotheses for Runnel

These hypotheses should be tested, not encoded as defaults:

- A typed schema with an opaque bytes field will reduce envelope overhead and
  make additive evolution clearer than extending the current Serde/JSON shape,
  but the generated-schema/tooling cost may outweigh the benefit for the first
  public client set.
- Bounded independent compression blocks will give a better total cost than
  per-record compression for correlated small messages, while large blocks
  will worsen replay memory or p99 latency.
- LZ4 will be the low-CPU/tail-latency comparator; low-level Zstandard will be
  the ratio/storage comparator. Neither should be assumed to win for random or
  already-compressed bytes.
- Storage and peer transport should be allowed to choose independently. A
  compressed producer/peer frame need not be the durable representation, and
  broker-side recompression may duplicate CPU work.
- A dictionary may help a single small-message family but will create more
  compatibility and operational risk for mixed tenants, rolling upgrades, and
  cold recovery than its ratio gain justifies unless evidence is strong.
- Batch wait, allocation/copy count, decode/replay work, and resource
  contention will matter to p99/p99.9 at least as much as encoded size.

## Acceptance evidence required later

No runtime benchmark is expected for this documentation-only change. Before a
future ADR accepts a format or default codec, collect evidence with the exact
source revision, codec/level, frame/block limits, durability point, topology,
resource limits, and measurement boundary attached to every result.

| Dimension | Initial values to measure |
|---|---|
| Logical payload | 100 B, 1 KiB, 16 KiB, and 1 MiB; random bytes, repeated text, JSON-like text, and already-compressed bytes. Include empty payload and values around every frame/block boundary. |
| Schema and framing | legacy text JSON, explicit base64 JSON bytes, a candidate binary envelope, and current peer JSON's byte-array representation. Keep schema-encoding bytes separate from codec-compressed bytes and outer framing. |
| Compression | none, LZ4 fast/independent, and low-level Zstandard with a fixed bounded window; compare per-record with independent 64 KiB and 256 KiB blocks. Record uncompressed fallback/decline cases and the framed break-even point; do not pick a universal threshold in advance. |
| Placement | For one fixed logical workload, compare public-wire, peer-wire, local retained-block, and selected clustered-persistence candidates independently. State which stages encode, decode, persist, and forward the transformed bytes; do not combine multiple placement changes in one result. |
| Workload | Durable single publishes and batches, batch response-timeout retry, restart replay, consume/ack, slow consumer, keyed grouped delivery, follower forwarding, and snapshot transfer as a separate case. Include one-record and multi-record batches with fixed record-count/byte caps and a stated maximum batch wait. |
| Topology and resources | Local engine and three-node cluster with quorum durability; record CPU/memory limits, storage medium, concurrency, same-stream versus many-stream load, and whether the host is otherwise idle. |
| Failure | Torn final frame, header/key/body bit flip, bad dictionary/codec ID, oversized stored/decoded/window length, expansion limit, follower restart, leader failure, interrupted snapshot transfer, and near/full storage. Test old-format prefix plus new-format tail and fail closed on complete unknown/corrupt frames. |
| Measures | Logical, schema-encoded, compressed/stored, and wire bytes at each boundary; encode/decode CPU; allocation count/bytes and copies where measurable; peak/RSS and in-flight codec memory; throughput; queue and batch wait; publish/ack p50/p99/p99.9/max; cold recovery/replay time and bytes; disk bytes written/read; peer fanout bytes; and failure classification. Report compression ratio alongside absolute bytes and these costs. |

The existing [benchmarking policy](../benchmarking.md) requires controlled
resources, matching workload semantics, and explicit treatment of inconclusive
results. The existing [testing workflows](../testing.md) provide the real
process, restart, cluster, and benchmark entry points. A codec microbenchmark
can explain a result, but cannot replace durable publish/replay and peer
transport evidence.

Include both closed-loop request/response load and a bounded open-loop offered
rate when evaluating batching or compression queues: client pacing can hide
queue growth and tail latency. For each point, report the configured limit and
observed high-water mark for concurrent encoded/decoded buffers, not just
process RSS. Keep public request/response, peer forwarding, Raft persistence,
state-machine journal, retained state, and snapshot artifacts separately
attributed; a saving in one is not evidence of a saving in another.

An ADR should not be proposed as accepted until evidence also covers:

- golden cross-version fixtures and interoperability in at least the intended
  client languages;
- bounded decoding/decompression and fuzz/fault-injection behavior;
- current-format restart, explicit refusal of obsolete local frames, retention, and
  conversion boundaries;
- per-record offsets, acknowledgements, redelivery, request identity, and
  dead-letter behavior through compressed blocks;
- peer handshake, unsupported capability, reconnect, snapshot, and leader/
  follower failure behavior; and
- metrics that expose codec choice, logical/stored/wire bytes, rejected
  frames, decompression failures, and resource pressure.

## Bounded next-step recommendation

1. **Freeze the current boundary with fixtures.** Capture representative
   RNL3 version-2 records, public text/base64 requests and responses,
   peer JSON frames, and journal tails. Add malformed, truncated, and checksum
   failure vectors to the focused test plan without changing defaults.
2. **Choose a bounded encoding extension in a future ADR.** Extend the current
   RNL3 frame or define a new frame family explicitly. Specify integer widths,
   endian convention, lengths, limits, checksum coverage, record/block index,
   reserved values, and writer/reader activation before implementation.
3. **Implement and test the uncompressed durable candidate first.** Keep
   the JSON/base64 public path, current RNL3 v2 behavior, peer JSON framing,
   Raft journal, and snapshots unchanged. Test opaque bytes, offset/replay/ack
   semantics, torn suffixes, complete corruption, restart, and bounded allocation.
4. **Measure each representation and placement separately.** Compare the
   current JSON/base64 and peer JSON byte-array sizes with an uncompressed
   binary-envelope candidate before attributing any reduction to compression.
   Then compare none, LZ4 independent blocks, and low-level bounded-window
   Zstandard independently at the public-wire, peer-wire, local retained-block,
   and selected clustered-persistence boundaries. Retain compressed bytes only
   when total framed bytes, CPU, memory, recovery, and tail-latency costs are
   acceptable at that named boundary; do not add dictionaries, high levels,
   broker recompression, or adaptive defaults yet.
5. **Design peer negotiation separately.** After the storage boundary is
   trustworthy, specify a preface/Hello and capability gate for peer control,
   forwarding, and snapshot traffic. Keep an old-peer refusal and reconnect
   path explicit; do not infer peer compatibility from durable-record parsing.

This sequence is intentionally narrow: it retires uncertainty about framing,
limits, corruption, and logical-byte preservation before multiplying it with
schema generation, compression policy, or rolling cluster upgrades.

## Unresolved decisions

- Is Protocol Buffers acceptable as a generated dependency for every intended
  client, or should CBOR be the public compatibility bridge?
- Which fields belong in the schema body versus the Runnel-owned outer frame,
  and does a durable block index provide sufficient offset-to-record lookup?
- Should the current RNL3 v2 frame be extended under a formally versioned
  contract or replaced by a new segment family? How should a future local
  layout be selected and activated?
- What are the maximum stored body, encoded envelope, logical payload,
  decompressed block, record count, and batch wait values at each boundary?
- Does the public binary path need one negotiated schema for requests,
  responses, and peer commands, or independent schema versions with a shared
  outer frame?
- Should durable blocks use independent or linked codec blocks, and can replay
  avoid decompressing unrelated records without an unbounded index?
- Which checksum is required for accidental corruption, and is a separate
  cryptographic digest needed for untrusted storage or diagnostics?
- Which codecs and levels are available in every supported client/server
  deployment, and how should unsupported required capabilities fail?
- What writer fence and rollback procedure prevents an old process from
  appending or acknowledging after a new generation activates?
- Which codec metrics and failure classes must make a node unhealthy,
  retryable, or permanently incompatible?

## Refactor and planning-record assessment

The touched subsystem was inspected for safe adjacent refactoring. Existing
planning records already cover the concrete adjacent issues: provisional JSON
and limited payload compatibility (TD-003), one-file local stream storage
(TD-002), storage-format compatibility (TD-007), incomplete end-to-end
benchmark coverage (TD-011), peer transport strategy (TD-012), and the
remaining module-ownership debt (TD-025). No new concrete shortcut or
retirement condition was found that is better represented by another
`docs/tech-debt.md` entry. The new current-code observations and acceptance
detail fit the existing [encoding/compression backlog outcome](../backlog.md#make-message-encoding-and-compression-evolvable),
which is updated in the same change; the tech-debt register remains unchanged.

## Verification commands

This is a document-only research/design change. From the repository root,
check the owned files and their external links with:

```text
git diff --check -- docs/backlog.md docs/research/message-encoding-and-compression.md docs/design/encoding-compression-day1-plan.md
python3 - <<'PY'
import re
import urllib.request
from pathlib import Path

paths = [
    Path("docs/research/message-encoding-and-compression.md"),
    Path("docs/design/encoding-compression-day1-plan.md"),
]
urls = sorted({
    url
    for path in paths
    for url in re.findall(r"\]\((https?://[^)]+)\)", path.read_text(encoding="utf-8"))
})
for url in urls:
    with urllib.request.urlopen(url, timeout=20) as response:
        if response.status >= 400:
            raise SystemExit(f"{response.status}: {url}")
print(f"checked {len(urls)} external links")
PY
git status --short -- docs/backlog.md docs/research/message-encoding-and-compression.md docs/design/encoding-compression-day1-plan.md
```

No Rust, integration, or benchmark command is required for this documentation
change. A future implementation is a design/research follow-up with storage,
public-contract, and peer-transport tests and with the benchmark evidence
listed above.
