# Message encoding and compression exploration

- Status: exploratory
- Last reviewed: 2026-08-24
- Scope: public request/response payloads, peer frames, and retained broker records

This document explores a path from Runnel's development-only JSON protocol and
unchecksummed retained-record frames to versioned binary encodings with optional
compression. It is not an accepted ADR and does not change the current wire or
storage format. An ADR should record the compatibility policy and exact format
before implementation starts.

## Current boundary and constraints

The [public request and response types](../../crates/runnel-protocol/src/lib.rs)
are Serde models carried as one JSON object per TCP line. The current public
`payload` is a Rust `String`; the server converts it to UTF-8 bytes before
calling the engine. The engine already models a message payload as `Vec<u8>`,
so the semantic boundary can become binary without changing delivery meaning.

The local [stream log](../../crates/runnel-core/src/lib.rs) uses a 28-byte
little-endian `RNL1` header followed by UTF-8 key bytes and raw payload bytes.
It records offset, timestamp, key length, and payload length, calls
`sync_data`, and truncates an incomplete suffix during recovery. It has no
checksum, format-version field, compression flag, or explicit uncompressed
length. The [peer transport](../../crates/runnel-raft/src/network.rs) uses a
u32 length prefix around JSON, while the [state-machine journal](../../crates/runnel-raft/src/lib.rs)
uses a u32 length prefix around versioned JSON. These formats have different
lifecycle and compatibility requirements and should not be replaced by one
universal serialization type.

The current [benchmark guidance](../testing.md) already treats 100-byte and
1-KiB messages, durability mode, ordering distribution, p50/p99/p99.9 latency,
CPU, and memory as relevant dimensions. Encoding work should extend that
evidence rather than infer a win from encoded size alone.

## Design goals

- Preserve the logical model: opaque payload bytes, optional UTF-8 ordering key,
  offsets, at-least-once delivery, explicit acknowledgement, and current
  durability semantics.
- Let a reader identify the format and safely bound every allocation before
  decoding or decompressing.
- Keep JSON available as a debuggable development representation during a
  migration, without making JSON the durable compatibility contract.
- Permit old and new records to coexist during recovery and rolling upgrades.
- Make compression an independently measurable policy, not an assumption in
  the message model.
- Keep the format implementable by future non-Rust clients and alternative
  engines.

Non-goals are schema-registry design, encryption/authentication, a universal
cross-product benchmark claim, and changing ordering or delivery guarantees.

## Public payload representation

The logical public message should be defined as:

```text
Message {
    stream: validated UTF-8 name,
    key: optional UTF-8 bytes used by broker ordering,
    payload: opaque bytes,
    published_at_ms: broker timestamp,
    offset: broker position,
    optional application metadata added by a later schema
}
```

The broker should not parse or transcode the payload. A binary protocol should
carry the payload as a length-delimited byte field. The legacy JSON adapter
continues to interpret the existing `payload` string as UTF-8 bytes and renders
only UTF-8 payloads. A future JSON representation for arbitrary bytes must use
an explicit representation such as a base64 field and a versioned response;
silently replacing the current string with base64 would break development
clients and make text less readable.

The envelope should distinguish the logical payload representation from a
transport or storage content coding. For example, `payload_encoding = bytes`
describes the application-visible value, while `compression = zstd` describes
an optional transformation of the encoded bytes. A content type is useful
metadata but should be opaque to the broker and deferred until a real client
use case requires it.

### Encoding alternatives

| Candidate | Strengths | Costs and fit for Runnel |
|---|---|---|
| Protocol Buffers | Compact typed wire format; numbered fields and length-delimited bytes support additive evolution and unknown-field skipping. The [encoding guide](https://protobuf.dev/programming-guides/encoding/) documents the TLV wire model, while the [evolution guide](https://protobuf.dev/programming-guides/editions/) documents field and unknown-field rules. | Requires schemas/code generation and a deliberate field-number policy. It is not self-describing, and the wire bytes are not a stable hash/canonical form by default. Strong candidate for public requests, responses, and peer commands. |
| CBOR | Standardized compact data model; supports bytes and maps, and [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html) explicitly targets extensibility without requiring version negotiation. | Dynamic maps and generic values leave more validation and allocation to Runnel; canonicalization and schema discipline would still be application policy. Good compatibility bridge or tooling format, less compelling as the long-lived typed peer contract. |
| MessagePack | Small binary analogue of JSON with broad implementations and easy Serde integration; [rmp-serde](https://docs.rs/rmp-serde/latest/rmp_serde/) documents binary bytes handling and the need to opt into efficient byte slices. | Struct/array versus map choices and Serde representation details can become an accidental schema. It is a reasonable prototype comparator, but its evolution rules are less explicit than Protocol Buffers for a public contract. |
| FlatBuffers / Cap'n Proto | Schema-driven access with little or no unpacking; [FlatBuffers evolution](https://flatbuffers.dev/evolution/) and [Cap'n Proto's schema rules](https://capnproto.org/language.html) provide compatibility mechanisms. | Verifiers, alignment/offset rules, generated code, and larger conceptual surface are valuable for zero-copy hot paths but unnecessary for the first request/response and append/replay slice. Their native messages also do not remove the need for Runnel-owned outer framing and limits. |

The recommendation for the public and peer schema is Protocol Buffers, subject
to a representative benchmark and an ADR. CBOR should remain the fallback if
schema generation or dynamic tooling becomes the dominant constraint. The
recommendation is about the envelope and command metadata; the application
payload remains an opaque bytes field in every candidate.

## Versioned framing

Use two layers:

1. a connection or segment frame that identifies the format, bounds the body,
   and carries checksum/compression metadata; and
2. a schema-encoded body containing a request, response, peer command, or
   logical record metadata.

The exact bytes require an ADR, but a durable record v2 should contain fields
equivalent to the following before key and payload bytes:

| Field | Purpose |
|---|---|
| magic and format version | Reject unrelated files and dispatch v1/v2 readers. |
| header length and flags | Permit additive header fields and distinguish record kinds. |
| encoded body length and logical body length | Bound reads, decompression, and allocation. |
| offset and published timestamp | Preserve broker semantics independently of physical position. |
| key length | Locate the key without interpreting payload bytes. |
| encoding and compression identifiers | Select the decoder explicitly; `none` is a valid value. |
| checksum type and checksum | Detect corruption over the header, key, and stored body. |

All integer widths, endianness, maximums, and reserved bits must be specified;
they must not be inferred from Rust layout. Unknown flags and versions should
fail closed. A header length allows future fields to be skipped only when the
reader can still validate the whole frame safely.

The outer frame should be Runnel-owned even if the body is Protocol Buffers or
another standard encoding. This gives Runnel a stable place for limits,
offsets, compression identifiers, and checksums, and prevents a library's
schema evolution rules from becoming the storage recovery policy.

## Version negotiation and mixed-format recovery

The existing JSON-lines connection remains protocol v1. A binary connection
should begin with an unambiguous fixed preface and a `Hello` exchange carrying:

- protocol major/minor versions supported;
- body encodings supported;
- compression algorithms and maximum decompressed frame size;
- maximum frame and batch sizes;
- optional capabilities such as batch publish or server-side payload bytes.

The server selects one mutually supported combination and returns a typed
failure when none exists. Negotiation is connection-scoped; a record's durable
format is still self-identifying. Do not silently reinterpret a malformed
binary preface as JSON or downgrade after application data has been exchanged.
The legacy JSON path may be selected by its existing line-oriented behavior
until a future protocol version is explicitly retired.

Recovery should recognize `RNL1` and v2 frames one frame at a time, but a new
writer must not append an unreadable frame to a file that an old binary can
still open. The safer migration shape is immutable format-tagged segments:

- v1 segments remain readable and are never rewritten in place during startup;
- a v2-capable reader can replay v1 and v2 segments in offset order;
- once all writers/readers for a stream or replica set advertise v2 support,
  new appends go to a v2 segment;
- compaction or export may later rewrite old segments, but only through a
  validated temporary file and atomic manifest update.

If a single-file mixed stream is eventually desired, every supported reader
must recognize both magic values and the writer must prove that no old process
can truncate an unknown suffix. Segment boundaries make that proof simpler and
make rollback possible: an old binary can continue to use old segments after a
v2 segment is drained, but it cannot consume new v2 data. A rolling upgrade must
therefore use capability discovery, drain or fence old writers before the first
v2 append, and retain a downgrade plan before enabling v2 by default.

For the JSON-based Raft log, state-machine journal, and snapshots, use the same
principles but separate migration gates. Consensus entries and snapshots are
not retained message records; a node must not interpret a successful data-log
upgrade as permission to decode a new consensus format. A replacement replica
must reject an unsupported snapshot or request a compatible snapshot rather
than partially applying it.

## Compression policy

Compression should normally operate on bounded batches or storage blocks, not
on each small message independently. A batch can expose first offset, record
count, encoded length, logical length, compression identifier, and checksum;
the broker can decompress one bounded block for replay while retaining an index
from offsets to blocks. This follows the useful property illustrated by
[Kafka's record-batch format](https://kafka.apache.org/26/implementation/message-format/):
records are batched and the batch carries compression attributes. It also keeps
compression scope separate from acknowledgement scope: a batch is a physical
unit, not a single delivery or acknowledgement unit.

Candidate policy:

- `none` for already small, incompressible, or latency-critical bodies;
- LZ4 for the low-CPU/tail-latency candidate; its [frame format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md)
  defines bounded blocks and optional block/content checksums;
- Zstandard at a low level for the ratio/storage/network candidate; [RFC 8878](https://www.rfc-editor.org/rfc/rfc8878.html)
  defines the interoperable format, frame content size, window bounds, and
  optional checksum, while the [format specification](https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md)
  explains the memory cost of larger windows;
- gzip only if an external interoperability requirement justifies its older
  throughput and ratio tradeoff. It is not the preferred broker default.

The encoder should sample or compress a bounded batch, retain compressed bytes
only when they beat the raw representation including framing overhead, and
record the selected algorithm in every frame. Initial implementations should
avoid shared dictionaries: they complicate segment portability, rolling
upgrades, memory accounting, and corruption recovery. If dictionaries later
win a measured workload, the frame must carry a dictionary identifier and the
reader must reject missing or incompatible dictionaries before allocation.

Compression selection is a policy input, not a client-visible semantic change.
Storage and peer transport may select different policies, and public payload
bytes must remain identical after decoding. A negotiated peer compression
choice must not cause a replica to persist a format it cannot recover.

### Trade-offs

| Choice | Likely benefit | Risk to measure |
|---|---|---|
| Per-record compression | Simple random access and isolated corruption | Repeats headers/windows, poor ratio for 100-byte messages, more allocations and codec calls. |
| Bounded batch compression | Better ratio and fewer calls; amortizes headers and checksums | Reads may decompress unrelated records; larger blocks increase memory and tail latency. |
| LZ4 | Low encode/decode CPU and predictable bounded work | Usually larger output than Zstandard; storage and network savings may be insufficient. |
| Zstandard low level | Better ratio at modest CPU; fast decompression | Encoder CPU, window memory, and queueing can raise p99 under contention. |
| High compression level or dictionary | Potential storage/network reduction | Higher CPU, memory, warm-up, portability, and tail-latency cost; not a first default. |

The first measured default should be no compression for the smallest messages
and a low Zstandard level for batches only if it improves end-to-end cost under
the configured CPU, memory, and storage limits. LZ4 must be measured as the
alternative when p99 or CPU efficiency dominates. This is a hypothesis, not an
accepted production default.

## Corruption detection and failure behavior

The outer frame should calculate a fast CRC-32C over a canonical byte sequence:
format fields that affect decoding, key bytes, and the stored (possibly
compressed) body. [RFC 3309](https://www.rfc-editor.org/rfc/rfc3309.html)
specifies CRC-32C's Castagnoli polynomial and validation procedure. The checksum
is for accidental corruption, not authenticity; authentication belongs to a
future security design. Codec-native checksums may remain enabled as a second
decoded-content check, but Runnel must not rely on a codec-specific checksum
for all encodings.

Before allocating or decompressing, validate magic, supported version, header
length, flags, stored length, logical length, key length, batch count, and all
configured limits. Reject integer overflow and decompression expansion beyond
the negotiated or local maximum. Validate the outer checksum before passing
bytes to the codec where possible.

Recovery behavior should distinguish:

- a short final header/body caused by an interrupted append: truncate only the
  incomplete suffix to the last complete frame, then sync the file;
- a complete frame with a bad checksum, impossible length, unsupported required
  feature, or invalid key encoding: fail recovery for that segment and surface
  corruption; do not silently skip a complete record;
- a bad replicated snapshot or journal frame: reject it and retry from a known
  good snapshot/log boundary, with an observable error and metric.

This is stricter than the current scanner's generic suffix truncation, but it
prevents a torn write and genuine media corruption from having the same silent
meaning. Tests must cover bit flips in header/key/stored body, truncated
compressed data, oversized logical lengths, unknown versions, and a crash after
the frame write but before the durability point.

## Batching and durability

Batching should be explicit at both the public and storage boundaries. A public
batch request can reduce framing, codec, syscall, and network overhead, while
the response must preserve one outcome or an unambiguous range/outcome per
request identity. A storage batch can assign consecutive offsets and write one
bounded frame/block, then call the selected durability primitive once. The
broker must not report an individual publish as durable until the batch's
durability point covers it.

Batch limits should be expressed in records, encoded bytes, logical bytes, and
wall-clock wait. Flush on whichever limit arrives first. Keep enough indexing
metadata to find a record without scanning or decompressing the entire stream.
Benchmark one-message batches, full batches, and mixed small/large records;
large batches that win throughput but inflate p99 are not an unconditional
improvement.

## Benchmark matrix

Every result should record source revision, codec/level, compression decision,
batch limits, durability point, storage medium, CPU/memory limits, topology,
and whether latency is measured at the public request or inside the engine.
Repeat each cell enough to report median and observed min/max, plus p50, p99,
and p99.9 where per-operation timing is available.

| Dimension | Values to start with |
|---|---|
| Logical payload | 100 B, 1 KiB, 16 KiB, 1 MiB; random, repeated text, JSON-like text, and already-compressed bytes |
| Encoding | Current JSON, Protocol Buffers candidate, CBOR comparator; raw bytes and UTF-8 text payloads |
| Compression | None, LZ4 fast, Zstandard low level; per-record versus 64/256-KiB bounded batches |
| Workload | Durable publish, publish/poll/ack, replay after restart, mixed batch sizes, slow consumer, and grouped keyed delivery |
| Topology | Local engine; three-node cluster with peer forwarding and quorum durability |
| Failure | Torn final frame, checksum bit flip, interrupted recovery, follower restart, snapshot transfer, and disk-full/near-limit behavior |
| Measures | Encoded/logical bytes, compression ratio, encode/decode CPU, allocations, peak/RSS memory, throughput, p50/p99/p99.9/max latency, recovery time, and bytes replayed |

The first implementation benchmark should compare the existing JSON peer frame
and `RNL1` record path against a binary frame with compression disabled. Only
after that baseline should compression be enabled in the same harness. The
existing [container and clustered benchmark workflows](../testing.md) are the
right end-to-end evidence; add focused codec/block microbenchmarks only to
explain a result, not as a substitute for durable and network measurements.

## Recommendation for the next implementation slice

Adopt the following as an implementation proposal, pending an ADR:

1. Define a Runnel-owned, versioned frame/codec interface with bounded lengths,
   explicit encoding and compression identifiers, and CRC-32C. Keep the
   logical payload as bytes and leave the current JSON-lines protocol unchanged.
2. Implement a read-only compatibility decoder for `RNL1` and a v2 durable
   record writer in new format-tagged segments. Start with `compression = none`.
   Add crash, mixed-segment, checksum, length-limit, and replay tests before
   changing the default writer.
3. Benchmark a Protocol Buffers envelope against JSON for peer requests and
   public payload bytes; benchmark CBOR only as the fallback comparator. Do not
   commit to a public binary protocol until the benchmark and cross-language
   fixture establish the compatibility boundary.
4. Add a binary connection preface and capability negotiation only after the
   v2 frame reader/writer is stable. Keep JSON as an explicit development
   protocol for at least one compatibility window.
5. Add bounded batch compression as a separate change, starting with LZ4 fast
   and Zstandard low level, and select a default only from end-to-end CPU,
   storage, memory, and tail-latency evidence.

This order makes the first risky change format recovery and observability,
rather than combining schema migration, compression, and public client
compatibility in one cutover.

## Unresolved decisions

- Is Protocol Buffers acceptable as a generated-schema dependency for every
  future client, or should CBOR be the public compatibility format?
- Does the JSON development protocol need an explicit base64 payload variant,
  or is arbitrary binary payload support restricted to the negotiated binary
  protocol at first?
- What exact v2 header fields, widths, endian convention, reserved bits, frame
  maximum, and segment naming/manifest rules should be accepted?
- Should the public wire, peer transport, retained records, journals, and
  snapshots share a schema codec while retaining separate frame formats, or
  should their codecs be versioned independently?
- Is a 64/256-KiB batch/block bound appropriate for the target memory and replay
  tail, and what index is needed for offset-to-block lookup?
- Should the initial checksum be CRC-32C only, or should a stronger digest be
  required for corruption diagnosis or future untrusted storage?
- Which compression threshold and level minimize total cost for the supported
  workloads, and should storage and peer transport choose independently?
- Can the cluster's capability gate guarantee that no old writer can append
  after v2 activation, and what rollback/downgrade procedure is required?
- Should format migration be lazy through mixed segments, an offline rewrite,
  or both; when is it safe to retire the v1 decoder?
- Which codec and compression metrics are required before enabling a new format
  by default, and which failures should make a node unhealthy versus retryable?

## Verification commands

This document-only change should be checked with the following commands from
the repository root:

```text
git diff --check -- docs/design/message-encoding-and-compression.md
python3 - <<'PY'
import re
import urllib.request
from pathlib import Path

path = Path("docs/design/message-encoding-and-compression.md")
text = path.read_text(encoding="utf-8")
urls = sorted(set(re.findall(r"\]\((https?://[^)]+)\)", text)))
for url in urls:
    with urllib.request.urlopen(url, timeout=20) as response:
        if response.status >= 400:
            raise SystemExit(f"{response.status}: {url}")
print(f"checked {len(urls)} external links")
PY
git status --short -- docs/design/message-encoding-and-compression.md
```

No Rust or benchmark command is required for a document-only change; the
implementation slice must run the focused recovery tests and the existing
`just bench`/`just bench-cluster` workflows before an ADR accepts a format.
