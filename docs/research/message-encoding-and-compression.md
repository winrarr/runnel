# Message encoding and compression study

- Status: research-backed exploratory study; not an accepted compatibility decision
- Last reviewed: 2026-09-04
- Baseline inspected: `6c666cd1a2d3e41c35d230a3156e57180a0f94fd`
- Evidence class: research/design
- Scope: public request/response payloads, retained message records, and the
  clustered peer transport

This document records the evidence and hypotheses behind the backlog outcome
[Make message encoding and compression evolvable](../backlog.md#make-message-encoding-and-compression-evolvable).
It does not change the current wire or storage format. The exact compatibility
policy, format bytes, and default codec require a future ADR after the focused
tests and measurements described here.

## Decision summary

The current system already has a useful binary-safety slice, but not an
evolvable encoding/compression contract:

- The public path is one UTF-8 JSON object per TCP line. Text payloads use the
  legacy `payload` string; arbitrary payload bytes use the explicit padded
  base64 `PublishBytes`, `PublishBatch`, `MessageBytes`, and replay variants.
- Local stream files are one `.log` file per stream. The reader dispatches
  between legacy `RNL1`, checksummed uncompressed `RNL2`, and request-aware
  checksummed `RNL3` frames. `RNL2` has encoding and compression fields, but
  only `bytes` plus `none` are currently accepted. `RNL3` carries request
  identity but has no compression metadata.
- Cluster peer RPCs remain a custom big-endian `u32` length prefix around JSON,
  capped at 64 MiB, with no protocol preface or codec negotiation. A peer frame
  can carry Raft control RPCs, forwarded operations, or a snapshot chunk, so
  one universal message-record format would couple unrelated compatibility
  lifecycles.
- Compression is not implemented on the public, retained-record, or peer
  paths. The current state-machine journal, checkpoint, and snapshot formats
  are separate JSON persistence boundaries and must not silently inherit a
  retained-message codec decision.

The bounded next step is an opt-in, uncompressed, Runnel-owned durable-frame
contract with golden fixtures and real restart/corruption tests. It should
preserve all current readers and public behavior, establish length/checksum
and recovery semantics, and measure the uncompressed framing cost. Only then
should bounded LZ4 and Zstandard experiments be added, followed by a separate
peer-transport negotiation design. No source supports claiming that one codec
or schema will win across Runnel's workloads.

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
[request dispatch](../../crates/runnel-server/src/main.rs),
[local stream log](../../crates/runnel-core/src/lib.rs),
[peer frame codec](../../crates/runnel-raft/src/network/framing.rs), and
[state-machine journal](../../crates/runnel-raft/src/state_machine_journal.rs).

| Boundary | Observed current behavior | What is still not established |
|---|---|---|
| Public client protocol | Serde-tagged JSON requests and responses are exchanged as one line per request. Incoming request bytes must be UTF-8. The configured request-frame limit is bounded above by 64 MiB and includes the JSON/base64 representation, not just decoded payload bytes. | No public binary protocol, version negotiation, compatibility range, or stable wire schema exists. A base64 request can consume substantially more wire space than its logical payload. |
| Public payloads | `Publish` accepts a UTF-8 `String`. `PublishBytes` and `PublishBatch` carry `BinaryPayload`, which is standard padded base64 in JSON and decodes to `Vec<u8>`. Responses choose the readable UTF-8 variant or an explicit base64 variant without changing logical bytes. | The current JSON path is a development representation, not a compact binary contract. The optional ordering key remains an application-visible UTF-8 string; changing key semantics to arbitrary bytes would be a separate decision. |
| Legacy local records | `RNL1` is a 28-byte little-endian header containing magic, offset, timestamp, key length, and payload length, followed by UTF-8 key bytes and raw payload bytes. It has no checksum, compression identifier, or format-version field. | A complete `RNL1` record does not provide corruption detection. Its lengths are bounded by file availability and integer arithmetic, but not by the versioned storage limits. |
| Versioned local records | `RNL2` version 1 is a 44-byte little-endian frame with flags, header length, stored/logical body lengths, offset, timestamp, key length, encoding `bytes`, compression `none`, reserved fields, and CRC-32C. Its reader rejects compressed records and requires the exact 44-byte header. | The versioned fields are an experimental boundary, not an evolvable contract: there is no accepted field-width/reserved-bit policy, segment generation, migration selector, or rolling-writer gate. |
| Request-aware local records | `RNL3` version 1 is a 48-byte little-endian frame with request-ID length and CRC-32C. It is used for public request identities and local dead-letter move identities. | `RNL3` has no encoding or compression identifiers. A future compressed request-aware record needs an explicit compatible version/family; reusing reserved bytes without a decision would make request deduplication and recovery ambiguous. |
| Local recovery | `StreamLog::open` scans complete frames, dispatches by magic, and truncates an incomplete suffix. A complete unsupported magic, invalid key encoding, impossible versioned field, or checksum mismatch fails recovery. Normal server startup uses `RNL1`; `VersionedV1` is an explicit core configuration/test path. | The one-file layout can contain different recognized frame families, but there is no cross-release mixed-writer guarantee, generation manifest, writer fence, or conversion/rollback procedure. Current read-forward behavior is useful evidence, not a release compatibility promise. |
| Peer transport | Peer requests and responses use a persistent or pooled TCP connection with a big-endian `u32` body length and JSON body. The frame cap is 64 MiB. `PeerRequest` covers Raft RPCs, forwarding, and data-group setup; snapshot chunks travel through the same outer framing. | There is no connection preface, version/capability handshake, codec negotiation, application checksum, or rule preventing a new writer from sending a body an older peer cannot interpret. JSON serialization of byte vectors also adds representation overhead. |
| Clustered persistence | The Raft log, state-machine journal, checkpoints, and snapshots have separate JSON formats and version/recovery rules. The journal uses a little-endian `u32` length plus JSON and truncates a partial final record; checkpoints and snapshots use atomic JSON writes. | A retained-message encoding decision does not establish consensus, journal, or snapshot compatibility. Those artifacts need independent migration gates and failure tests. |

The local engine preserves the important semantic boundary: payloads are
`Vec<u8>` internally, offsets are logical record positions, and consumer
acknowledgement state is persisted independently of the physical payload
bytes. A compressed block therefore cannot become an acknowledgement unit or
change the ordering and redelivery model.

## Constraints for an evolvable design

These constraints apply to future proposals; they do not imply that the
current implementation already satisfies them.

1. **Byte preservation:** an opaque payload must round-trip byte-for-byte. The
   broker must not parse, transcode, normalize, or infer an application
   schema. UTF-8 validation applies to fields that are defined as text, not to
   payload bytes.
2. **Semantic preservation:** offsets, published timestamps, optional UTF-8
   ordering keys, at-least-once delivery, acknowledgement ordering, retry
   attempts, request identities, and dead-letter identity must retain their
   current logical meaning.
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
6. **Upgrade safety:** old data must remain readable for the documented
   compatibility window. New writers need an explicit capability/fencing gate;
   parsing one new frame successfully is not proof that old writers can append
   beside it or that rollback is safe.
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
  configuration](https://kafka.apache.org/41/configuration/producer-configs/),
  [record-batch format](https://kafka.apache.org/41/implementation/message-format/).
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
  gives a resource-bounded window-sizing profile.
- NATS separates its byte-counted client payload from application data
  formats: [client protocol](https://docs.nats.io/reference/protocols/client)
  and [message structure](https://docs.nats.io/using-nats/developer/sending/structure).
  JetStream's `Compression` setting is an at-rest file-store setting (`s2` or
  none), not evidence of a compressed client payload contract: [stream
  configuration](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md).

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
| gzip or Snappy | Kafka/Redpanda/NATS references make them relevant interop comparators; Snappy is the JetStream at-rest reference. | No current Runnel requirement calls for them. Adding more codecs expands capability negotiation, test matrices, security review, and maintenance. Include only for a measured workload or external interoperability need. |

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

### Durable records and mixed-format recovery

The current reader's `RNL1`/`RNL2`/`RNL3` dispatch is a useful compatibility
fixture. It is not enough to promise rolling upgrades because the current
one-file writer has no generation selector or old-writer fence. The safer
future migration shape is format-tagged immutable segments with a small
validated generation/manifest selector:

- retain old segments and keep their readers;
- write the new format only to a new segment family after an explicit writer
  capability gate;
- validate complete frames before exposing records;
- convert/compact only through a temporary validated output and an atomic
  selector update; and
- define what an old binary does when the active generation contains a frame
  it cannot read: fail closed or use a tested fallback, never appear empty.

If a single file eventually mixes frames, every supported writer and recovery
path must recognize every permitted magic/version and prove that an old
writer cannot truncate an unknown suffix. Segment boundaries make that proof
and rollback boundary easier. The storage-upgrade proposals provide the
broader generation, fence, and rollback context: [policy](../design/storage-upgrade-policy.md)
and [safety plan](../design/storage-upgrade-safety-plan.md).

### Recovery and corruption rules

Before allocating key, body, batch, or decompression buffers, validate all
lengths and limits. For a durable block:

- a short final header/body that can only be a torn append may be truncated to
  the last complete frame and synchronized;
- a complete frame with a bad checksum, impossible length, unknown required
  version/codec/dictionary, invalid text field, or decompression expansion
  beyond policy must fail recovery and surface corruption/unsupported format;
- a frame must not be silently skipped merely because its payload is
  unreadable; and
- a bad snapshot or journal record must follow its consensus recovery boundary,
  not be treated as a retained-message record.

The acceptance tests need bit flips in header, key, request ID, and stored
body; truncated compressed blocks; oversized stored/logical/window lengths;
unknown IDs and flags; a crash at the write/sync boundary; and restart with
both old and new records. CRC-32C is suitable for accidental-corruption
coverage if retained, but it does not provide authenticity or malicious-input
protection by itself.

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
| Logical payload | 100 B, 1 KiB, 16 KiB, 1 MiB; random bytes, repeated text, JSON-like text, and already-compressed bytes |
| Public representation | legacy text JSON, explicit base64 JSON bytes, candidate binary envelope; record decoded payload bytes and wire bytes separately |
| Compression | none, LZ4 fast/independent, Zstandard low level/bounded window; per-record versus bounded 64 KiB and 256 KiB blocks |
| Workload | durable publish, publish batch, replay after restart, consume/ack, slow consumer, keyed grouped delivery, follower forwarding, and snapshot transfer as a separate case |
| Topology | local engine and three-node cluster with quorum durability |
| Failure | torn final frame, header/key/body bit flip, bad dictionary/codec ID, oversized logical/window length, follower restart, leader failure, interrupted snapshot transfer, and near/full storage |
| Measures | logical, encoded, stored, and wire bytes; compression decision/ratio; encode/decode CPU; allocations/copies; peak/RSS memory; throughput; batch wait; p50/p99/p99.9/max latency; recovery time; bytes replayed; and failure classification |

The existing [benchmarking policy](../benchmarking.md) requires controlled
resources, matching workload semantics, and explicit treatment of inconclusive
results. The existing [testing workflows](../testing.md) provide the real
process, restart, cluster, and benchmark entry points. A codec microbenchmark
can explain a result, but cannot replace durable publish/replay and peer
transport evidence.

An ADR should not be proposed as accepted until evidence also covers:

- golden cross-version fixtures and interoperability in at least the intended
  client languages;
- bounded decoding/decompression and fuzz/fault-injection behavior;
- old/new readers, writer fencing, mixed-format restart, retention, and
  rollback boundaries;
- per-record offsets, acknowledgements, redelivery, request identity, and
  dead-letter behavior through compressed blocks;
- peer handshake, unsupported capability, reconnect, snapshot, and leader/
  follower failure behavior; and
- metrics that expose codec choice, logical/stored/wire bytes, rejected
  frames, decompression failures, and resource pressure.

## Bounded next-step recommendation

1. **Freeze the existing boundary with fixtures.** Capture representative
   `RNL1`, `RNL2`, and `RNL3` bytes, public text/base64 requests and responses,
   peer JSON frames, and journal tails. Add malformed, truncated, and checksum
   failure vectors to the focused test plan without changing defaults.
2. **Choose one candidate durable-frame family in a future ADR.** Reuse the
   current `RNL2`/`RNL3` work only if their exact-header and request-identity
   limitations are intentionally addressed; otherwise define a new
   format-tagged segment family. Specify integer widths, endian convention,
   lengths, limits, checksum coverage, record/block index, reserved values,
   and writer/reader activation before implementation.
3. **Implement and test only the uncompressed durable candidate first.** Keep
   the JSON/base64 public path, default `RNL1` writes, current recognized
   readers, peer JSON framing, Raft journal, and snapshots unchanged. Make the
   candidate opt-in and test opaque bytes, offset/replay/ack semantics, torn
   suffixes, complete corruption, restart, and bounded allocation.
4. **Measure that baseline, then add codec experiments.** Compare none, LZ4
   independent blocks, and low-level bounded-window Zstandard on the same
   local and clustered workloads. Retain compressed bytes only when framing,
   CPU, memory, recovery, and tail-latency costs are acceptable; do not add
   dictionaries, high levels, broker recompression, or adaptive defaults yet.
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
- Should `RNL2`/`RNL3` be extended under a formally versioned contract or
  retired behind a new segment family? How are current one-file logs selected,
  migrated, and rolled back?
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
remaining module-ownership debts (TD-024/TD-025). No new concrete shortcut or
retirement condition was found that is better represented by another
`docs/tech-debt.md` entry, so that file is intentionally unchanged.

## Verification commands

This is a document-only research/design change. From the repository root,
check the owned files and their external links with:

```text
git diff --check -- docs/research/message-encoding-and-compression.md docs/design/encoding-compression-day1-plan.md
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
git status --short -- docs/research/message-encoding-and-compression.md docs/design/encoding-compression-day1-plan.md docs/tech-debt.md
```

No Rust, integration, or benchmark command is required for this documentation
change. A future implementation is a design/research follow-up with storage,
public-contract, and peer-transport tests and with the benchmark evidence
listed above.
