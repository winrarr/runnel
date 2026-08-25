# Encoding and compression implementation plan

- Status: exploratory implementation plan
- Related research: [Message encoding and compression study](../research/message-encoding-and-compression.md)
- Scope: the first compatibility-safe implementation slice

This plan turns the research into an implementation boundary without making
the proposed binary format or codec policy an accepted compatibility decision.
The current JSON-lines protocol and `RNL1` records remain supported while the
new path is measured and exercised through restart and corruption tests.

## Intended boundary

Keep four concepts distinct:

1. logical message bytes, which are opaque to the broker;
2. an encoded envelope containing broker metadata and message bytes;
3. an optional transport representation; and
4. an optional durable representation.

A choice at one boundary must not silently determine the others. In
particular, a compressed durable block is a physical storage unit, not an
acknowledgement or delivery unit, and consumers must observe the same logical
payload after decoding.

## Day 1 proposal

The first implementation slice should establish measurable format and
recovery behavior before adding compression or changing the public client
contract:

- introduce a format-tagged, length-bounded durable frame family alongside
  `RNL1`;
- include explicit version, header/body lengths, logical length, encoding and
  compression identifiers, and a corruption checksum;
- start with an uncompressed body so bounds, checksum validation, mixed-format
  recovery, and restart behavior can be tested independently;
- keep JSON-lines as the development-compatible protocol and represent binary
  application payloads as opaque bytes in the new boundary;
- use immutable format-tagged segments for migration, so old segments remain
  readable and old writers cannot accidentally truncate an unknown suffix;
- make the new writer opt-in until crash, torn-suffix, corruption, size-limit,
  replay, and rolling-read tests pass.

The exact field widths, segment naming, activation gate, checksum choice, and
public binary schema require a later ADR. Rust layout or a serialization
library's defaults must not become the compatibility contract by accident.

## Day 2 experiments

After the uncompressed frame has a verified baseline, measure bounded-batch
compression for representative workloads:

- no compression, LZ4-fast, and low-level Zstandard;
- 100 B, 1 KiB, 16 KiB, and 1 MiB payloads with repetitive, JSON-like,
  random, and already-compressed content;
- per-record versus bounded 64 KiB and 256 KiB blocks;
- durable publish, replay, peer replication, slow consumers, recovery, and
  three-node operation;
- throughput, p50/p99/p99.9 latency, CPU, peak memory, encoded bytes, recovery
  time, and bytes replayed.

Compression should be retained only when it wins after framing overhead. Do
not introduce shared dictionaries, high compression levels, broker-side
recompression, or adaptive policy as defaults until measurements show that
their CPU, memory, recovery, and rolling-upgrade costs are justified.

## Exit criteria for an ADR

An accepted format decision should be based on evidence and specify:

- the durable and wire compatibility versions and their negotiation rules;
- maximum encoded, logical, decompressed, and batch sizes;
- checksum coverage and failure behavior for incomplete versus corrupt
  frames;
- mixed-format recovery and downgrade/rolling-upgrade behavior;
- the selected schema encoding, if a binary public or peer protocol is added;
- codec selection rules, metrics, and the workloads for which compression is
  enabled or declined.

The implementation is not ready to replace the current defaults until the
decision is recorded and the benchmark matrix demonstrates an acceptable
trade-off for latency, throughput, resource use, and recovery.
