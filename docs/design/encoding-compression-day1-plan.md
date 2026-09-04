# Encoding and compression implementation plan

- Status: exploratory implementation plan; not an accepted compatibility decision
- Last reviewed: 2026-09-04
- Baseline inspected: `6c666cd1a2d3e41c35d230a3156e57180a0f94fd`
- Evidence class: design/research
- Related research: [Message encoding and compression study](../research/message-encoding-and-compression.md)
- Scope: the first bounded implementation slice for the message-encoding and
  compression backlog outcome

This plan is advisory. It does not authorize a public binary protocol, a new
durable default, a rolling-upgrade promise, or a codec choice. The current
JSON-lines protocol, base64 binary payload variants, `RNL1`/`RNL2`/`RNL3`
readers, peer JSON frames, and clustered JSON persistence remain the observed
baseline until a future ADR and implementation evidence say otherwise.

## Boundary to protect

Keep these four representations distinct:

1. **Logical message bytes:** opaque application payload, optional UTF-8
   ordering key, logical offset, timestamp, and delivery identity.
2. **Encoded envelope:** a schema representation of broker metadata and
   message bytes.
3. **Wire representation:** a bounded client or peer frame, possibly
   negotiated and compressed for that connection.
4. **Durable representation:** a self-identifying stream frame or block whose
   checksum, sync point, recovery, and migration rules are independent of the
   wire choice.

A compressed durable block is a physical storage unit, not a delivery,
acknowledgement, or ordering unit. A peer's compressed snapshot or forwarded
request must not cause a replica to persist a representation it cannot decode.

## Current observed baseline

| Area | Current behavior relevant to this plan |
|---|---|
| Public protocol | UTF-8 JSON lines. Text uses `payload`; arbitrary bytes use explicit padded-base64 request/response variants. There is no binary handshake or negotiated codec. |
| Local stream log | One `.log` file per stream. `RNL1` is legacy raw and unchecksummed. `RNL2` is version 1, checksummed, uncompressed, and opt-in through the core API. `RNL3` is version 1, checksummed, and adds request identity without compression metadata. The reader dispatches by magic and truncates an incomplete final suffix. |
| Peer transport | Big-endian `u32` length prefix around JSON, 64 MiB body limit, persistent/pool connections, and no preface or capability negotiation. The same outer frame carries control RPCs, forwarding, and snapshot chunks. |
| Clustered persistence | Raft log, state-machine journal, checkpoints, and snapshots are separate JSON formats and recovery paths. They are not part of the first retained-message codec experiment. |
| Existing planning boundaries | [Storage-upgrade policy](storage-upgrade-policy.md) owns generation, fence, migration, and rollback questions. [TD-003](../tech-debt.md#td-003-provisional-json-lines-protocol-and-limited-payload-compatibility), [TD-007](../tech-debt.md#td-007-storage-format-compatibility-is-not-yet-defined), [TD-011](../tech-debt.md#td-011-end-to-end-benchmark-coverage-is-incomplete), and [TD-012](../tech-debt.md#td-012-peer-rpc-connection-strategy-remains-incomplete) track adjacent open debt. |

The current `RNL2` fields are valuable test material, but its exact 44-byte
header and `compression = none` restriction do not by themselves establish an
evolution policy. `RNL3` has a separate request-identity layout and must not be
treated as an interchangeable version of `RNL2`.

## Design guardrails

Future implementation work must:

- preserve opaque payload bytes and current delivery semantics;
- identify encoding, compression, dictionary, and format version explicitly;
- reject unknown required versions, flags, codecs, and dictionaries before
  unbounded allocation or decompression;
- distinguish incomplete final writes from complete corrupt frames;
- keep logical offsets and request identities independent of physical block
  layout;
- keep old readers and the JSON/base64 development path until an explicit
  compatibility decision retires them;
- use a format-tagged segment/generation and writer-fence design before
  claiming rolling-upgrade or downgrade support; and
- expose logical, encoded, stored, and wire byte counts plus codec failures so
  later measurements cannot confuse compression ratio with total cost.

The schema candidates and codec facts are compared in the [research study](../research/message-encoding-and-compression.md#encoding-alternatives),
including [Protocol Buffers](https://protobuf.dev/programming-guides/encoding/),
[CBOR](https://www.rfc-editor.org/rfc/rfc8949.html), [LZ4 frames](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md),
and [Zstandard frames](https://www.rfc-editor.org/rfc/rfc8878.html).

## Day 1: uncompressed durable-frame slice

### Objective

Establish one bounded, checksummed, self-identifying durable-frame candidate
and its recovery evidence without changing public or peer defaults and without
adding compression. The result should make the format cost and compatibility
boundary measurable before codec complexity is introduced.

### Work items

1. **Freeze current behavior in fixtures.**

   Capture representative bytes and logical results for:

   - legacy `RNL1` records, checksummed `RNL2` records, and request-aware
     `RNL3` records;
   - empty, UTF-8, binary, large, and already-compressed payloads through the
     current public JSON/base64 path;
   - current peer JSON frames, including a forwarded binary payload and a
     bounded snapshot chunk; and
   - valid journal records, a partial journal tail, and complete invalid JSON.

   These fixtures document observed behavior. They do not bless the current
   layouts as cross-release contracts.

2. **Choose the candidate family and exact descriptor in an ADR.**

   Before writing a new format, decide whether the candidate formally extends
   the `RNL2` family or uses a new format-tagged segment family. Do not add an
   untracked magic or infer a compatibility contract from Rust struct layout.
   The descriptor proposal should make these fields unambiguous:

   - magic, format/schema version, header length, flags, and reserved values;
   - stored-body length and pre-compression encoded-body length;
   - logical record count and offset range or bounded offset index;
   - UTF-8 key/request-ID lengths where those fields are outside the body;
   - encoding, compression, and dictionary identifiers, with `none` explicit;
   - checksum algorithm and coverage, including checksum-field zeroing; and
   - maxima for header, key, request ID, encoded body, decoded body, record
     count, and block.

   Exact widths, endian convention, segment names, and activation rules are
   intentionally not accepted by this plan. They belong in the ADR.

3. **Implement only `compression = none` first.**

   The candidate writer should be opt-in and should preserve the existing
   default writer. The candidate reader should validate all descriptor fields,
   lengths, reserved values, key encodings, and checksums before exposing a
   logical record. Store opaque payload bytes without a text conversion.

4. **Define physical-block versus logical-record behavior.**

   If a candidate block contains multiple records, retain enough bounded index
   information to locate an offset without scanning all history. Assign and
   expose one logical offset per record. A batch sync point must cover every
   publish reported as durable; an individual acknowledgement must never be
   inferred from a block-level write alone.

5. **Specify failure and recovery oracles.**

   - Truncate only a provably incomplete final frame/block to the last complete
     boundary and synchronize the file.
   - Fail recovery on a complete checksum mismatch, impossible length,
     unsupported required version/codec/dictionary, invalid text field, or
     decompressed length beyond policy. Do not silently skip the complete
     record.
   - Preserve a usable source generation until target validation and activation
     are complete. Do not claim rollback merely because old files remain on
     disk.

6. **Add focused real-process and storage tests.**

   Required cases include opaque-byte round trip, empty payload, maximum and
   over-limit lengths, malformed flags/version, header/key/body bit flips,
   truncated final frame, crash between write and sync, restart replay,
   request-ID deduplication, offset replay, normal acknowledgement, grouped
   delivery, and dead-letter movement. The public protocol remains the current
   JSON/base64 path, so network tests should prove that this slice does not
   alter current client behavior.

7. **Add format-level observability without claiming a win.**

   Record format/version, logical bytes, encoded bytes, stored bytes, frame or
   block count, rejected-frame reason, recovery truncation, and checksum or
   decode failure. Keep metrics bounded and avoid logging payload contents.

### Day 1 exit gate

The slice is ready for codec experiments only when all of the following are
true:

- current `RNL1`/`RNL2`/`RNL3` fixtures remain readable as documented;
- candidate records preserve payload bytes, keys, offsets, timestamps, request
  identities, acknowledgements, redelivery, replay, and dead-letter semantics;
- every allocation and decompression boundary is bounded before work begins;
- torn final writes are recoverable while complete corruption is observable;
- restart and real-process tests cover local durable behavior;
- old/default public and peer paths are unchanged; and
- an uncompressed measurement reports framing, copying, CPU, memory, recovery,
  and tail-latency cost under a stated workload and resource budget.

This gate is evidence for a future decision, not a compatibility promise. If
the candidate fails, revise the descriptor or recovery model before adding a
codec.

## Day 2: bounded codec experiments

Once the uncompressed candidate passes its focused gate, measure the same
durable and peer-relevant workloads with:

- no compression;
- LZ4 fast with independent bounded blocks; and
- Zstandard at a low level with an explicitly bounded window and content size.

Start with one-record, 64 KiB, and 256 KiB physical batches/blocks. Include
100-byte, 1 KiB, 16 KiB, and 1 MiB payloads; random bytes, repeated text,
JSON-like text, and already-compressed bytes. Record whether compression was
declined when framing overhead erased the benefit.

Measure separately:

| Path | Required evidence |
|---|---|
| Local durable publish | publish latency at the durability point, logical/stored bytes, encode CPU, allocations, peak memory, and sync behavior |
| Local replay/restart | recovery time, bytes scanned/replayed, decompression CPU, memory, offset lookup, and corruption classification |
| Public protocol | JSON/base64 versus candidate binary envelope, logical versus wire bytes, request/response p50/p99/p99.9, and batch wait |
| Cluster forwarding/replication | follower ingress, peer frame bytes, quorum latency, peer decode CPU, reconnect/unsupported capability behavior, and node resource use |
| Snapshot transfer | transfer size/time and install/recovery behavior; do not mix these results into retained-record conclusions |
| Delivery semantics | per-record offsets, grouped ordering, ack/redelivery, request identity, and dead-letter behavior through a physical block |

Use controlled local and three-node runs with the exact source revision,
durability mode, topology, CPU/memory limits, storage medium, concurrency,
payload distribution, block limits, and repetition count attached. The
[benchmarking policy](../benchmarking.md) governs stability and handoff
language. A result that improves bytes but worsens p99, recovery, memory, or
CPU is mixed evidence, not an unconditional win.

Do not make shared dictionaries, linked blocks, high compression levels,
broker-side recompression, or adaptive selection defaults in Day 2. Each adds
dictionary distribution, memory, recovery, or policy complexity that requires
its own evidence.

## Peer-transport follow-up

Peer encoding is a separate slice after the durable candidate is trustworthy.
It should define:

- an unambiguous connection preface and `Hello` exchange;
- protocol/schema versions and codec capabilities selected per connection;
- maximum stored frame, decoded envelope, decompressed block, and batch sizes;
- explicit dictionary IDs only if a distribution/retirement protocol exists;
- no silent downgrade or codec change after application data begins;
- separate compatibility gates for Raft control, forwarded operations, and
  snapshot chunks; and
- reconnect behavior that repeats negotiation and fails clearly when no
  mutually supported choice exists.

Keep control messages uncompressed in the first experiment unless their cost
is measured. Do not replace the Raft journal or snapshot persistence format as
part of a peer-wire experiment. A peer-wire change requires real three-node
process tests for follower forwarding, leader change, snapshot transfer,
unsupported capability, reconnect, and recovery.

## ADR gate

An accepted decision must record, at minimum:

- the public, peer, durable, journal, checkpoint, and snapshot compatibility
  scope for the decision;
- exact format versions, field widths, endian convention, frame/block limits,
  reserved values, checksum coverage, and failure behavior;
- selected schema encoding and its generated-code/interoperability policy, if
  a binary public or peer contract is introduced;
- compression algorithms, levels/window limits, batch/block scope, decline
  threshold, dictionary policy, and metrics;
- mixed-format reads, rolling-upgrade writer fencing, activation, rollback, and
  old-binary refusal behavior;
- logical-record indexing and proof that compression does not change offset,
  acknowledgement, ordering, replay, retry, or dead-letter semantics; and
- the benchmark matrix, resource budget, raw artifacts, repeatability status,
  and known coverage gaps supporting the choice.

The ADR must cite the primary sources in the [research study](../research/message-encoding-and-compression.md)
and distinguish those sourced facts from Runnel-specific inference. Until
then, this plan remains exploratory.

## Explicit non-goals for this slice

- no implementation is made by this document-only change;
- no current public or peer default is replaced;
- no public binary protocol or schema dependency is accepted;
- no encryption/authentication or cryptographic integrity design is selected;
- no claim is made about compression or latency improvement;
- no automatic migration, downgrade, or rolling-upgrade path is promised; and
- no change is made to Raft journal, checkpoint, snapshot, or consensus
  semantics.

## Refactor and planning-record assessment

The current code's adjacent structural issues were inspected. The existing
technical-debt register already captures the relevant future work: local
one-file storage, provisional JSON/base64 compatibility, storage migration,
benchmark coverage, peer connection strategy, and codec ownership remaining
inside the large local/clustered modules. No additional concrete debt item with
its own distinct retirement condition was warranted by this documentation
slice, so `docs/tech-debt.md` is intentionally unchanged.

## Verification

This document-only change is classified as research/design. Run:

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

No Rust or benchmark run is required for this document-only change. The future
implementation slice must run the focused storage/recovery and real-process
network tests before its ADR can accept a format; codec defaults additionally
require the controlled local and clustered benchmark evidence above.
