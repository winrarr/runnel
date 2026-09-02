# Protocol compatibility and evolution

- Status: proposed design; not an accepted compatibility contract
- Date: 2026-09-02
- Scope: public client/broker requests and responses
- Related debt: TD-003 and [Make client interactions dependable and evolvable](../backlog.md#make-client-interactions-dependable-and-evolvable)

This note defines the smallest useful compatibility policy for the current
wire and a direction for its next evolution. It records observed behavior and
proposals separately. Nothing here closes TD-003: the versioned runtime,
interoperability matrix, and upgrade/recovery evidence are still required.

## Policy summary

Keep the existing line-delimited JSON mode as provisional `v1`. It has no
negotiated version and no promise that an old client can consume every future
operation. Preserve its existing text and explicit binary forms while clients
and servers are being made dependable.

For the next protocol, use a connection-scoped, versioned, length-delimited
envelope. Select it with an unambiguous connection preface and a Hello exchange
that advertises protocol versions, capabilities, payload encodings, and frame
limits. Do not switch framing or downgrade after application data has been
exchanged. The exact preface, schema codec, and field layout require a later
ADR and implementation tests.

The logical message remains an opaque payload, an optional UTF-8 key, a broker
offset, and delivery metadata. Wire encoding, compression, and durable storage
are separate choices. A new wire representation must not alter acknowledgement,
redelivery, ordering, durability, or the meaning of an offset.

## Current v1: observed boundary

The [protocol types](../../crates/runnel-protocol/src/lib.rs) are Serde enums
tagged by `op` for requests and `type` for responses. The server reads one
object per TCP line and writes one response line. The reusable [client](../../crates/runnel-client/src/lib.rs)
serializes one request, waits for one response, and does not pipeline or retry
automatically. The current operation surface is:

| Direction | Tags | Compatibility-relevant fields |
| --- | --- | --- |
| Request | `create_stream`, `publish`, `publish_bytes`, `publish_batch`, `poll`, `poll_group`, `ack`, `ack_group`, `health` | UTF-8 names and keys; text `payload`; explicit `payload_base64`; optional publish `request_id`; batch records and ordered outcomes |
| Response | `stream_created`, `published`, `publish_batch`, `message`, `message_bytes`, `empty`, `acknowledged`, `health`, `error` | offsets, text or binary payload, optional group delivery fields, acknowledgement state, stable error `code` and human-readable `message` |

The current wire rules are deliberately narrow:

- A complete frame is a JSON object followed by LF; the server also accepts a
  CR immediately before LF. JSON object member order is not a contract, and
  senders must use unique member names as advised by [RFC
  8259](https://www.rfc-editor.org/rfc/rfc8259.html).
- Struct-bearing request variants and `PublishBatchRecord` use
  `deny_unknown_fields`; the unit `health` variant currently accepts extra
  members because of the deserializer shape. Unknown request tags, malformed
  JSON, malformed base64, and contradictory text/binary fields are rejected
  before engine execution.
- Response objects currently ignore unknown members in known variants, while
  an unknown response tag is rejected. This asymmetry is tested because it is
  current behavior, not because it is a final policy.
- Missing optional request IDs and optional response delivery fields remain
  readable. A serializer currently emits `request_id: null` on publish
  requests and omits absent optional response fields.
- `payload` is UTF-8 text. `publish_bytes` and `message_bytes` carry exact
  application bytes as standard padded base64 in `payload_base64`; the legacy
  text shape is not silently reinterpreted as base64. Base64 expansion counts
  against the current request-frame limit; responses have no negotiated
  maximum yet and remain subject to current write behavior.
- `request_id` is an application-provided publish identity. It is present on
  single publishes and batch records, is not echoed in the current response,
  and is not a general response correlation ID.

The current [server retry test](../../crates/runnel-server/tests/client_retry.rs)
demonstrates the important outcome boundary: after a response is lost, the
client reports an unknown publish attempt, and explicitly replaying the same
publish identity returns the existing result without a duplicate. A connection
failure before any request bytes are sent is retryable; a write, timeout, EOF,
or cancellation after writing may have reached the broker and is unknown. The
client intentionally leaves retry policy to the caller.

These facts describe the implementation at this baseline. They are not a
claim that arbitrary v1 clients and future servers interoperate.

## Proposed v2 compatibility policy

### Version and framing

The v1 listener remains available while v2 is introduced. A v2 connection
starts with a reserved, unambiguous preface and then exchanges `Hello` messages
before application requests. Hello negotiates a mutually supported protocol
major/minor range and capabilities such as binary payloads, publish batches,
maximum frame and batch bytes, and supported compression. The server rejects
no-overlap with a typed unsupported-version result and closes or drains the
connection; it must not guess a format from malformed bytes.

V2 frames are length-delimited and bounded before allocation. Each application
frame carries an opaque `correlation_id`; the response repeats it. This is
needed for matching responses if v2 later permits concurrent in-flight
requests. It is distinct from the operation-level `request_id`, which remains
the stable producer identity used to resolve an ambiguous publish. A retry may
use a new correlation ID and must reuse the same request ID when deduplication
is requested.

Negotiation is connection-scoped. A reconnect performs Hello again because
the peer may have been upgraded or rolled back. A v2 connection never changes
frame format midstream, and a v1 client never sends a v2 operation merely
because a server understands it.

This direction follows two useful reference patterns. [Kafka's protocol
guide](https://kafka.apache.org/41/design/protocol/) versions each API, has a
client select the highest mutually supported version, returns the response
shape for the requested version, and repeats discovery after reconnect. [NATS's
client protocol](https://docs.nats.io/reference/protocols/client) keeps a
simple text protocol but sends an initial `INFO` with protocol capabilities and
limits, then uses explicit byte counts for message payloads. Runnel needs the
first pattern for schema evolution and the second pattern's inspectable
capability/limit boundary; its v1 JSON mode already exists and cannot gain a
handshake without changing current clients.

### Compatible changes

The following are compatible within a negotiated v2 version only when the
operation's documented meaning is unchanged:

- Add optional response fields that older clients can ignore.
- Add optional request fields only when omission has a safe, documented
  default, and only send them after the peer advertises the capability or
  version that gives them meaning.
- Add an operation or response variant behind capability discovery; clients
  must not send it when the peer does not advertise it.
- Add payload metadata that does not change payload bytes, delivery semantics,
  or limits.
- Increase a limit only when it is advertised and selected per connection;
  clients must continue to respect the negotiated lower limit.

An additive change is not automatically safe just because a parser can skip
it. A client must not assume that a field ignored by an older peer was applied.
Requests that require a new behavior must fail with an explicit unsupported
capability/version result or use a compatible fallback.

### Incompatible changes

The following require a new operation version or a new major protocol version:

- changing the meaning, type, encoding, requiredness, or default of an
  existing field;
- renaming or reusing a discriminator or schema field number;
- changing text payload bytes into base64, changing base64 alphabet/padding, or
  changing the logical payload bytes after decode;
- changing offset, ordering, acknowledgement, redelivery, durability, batch
  atomicity, or retry/unknown-outcome semantics;
- introducing a new required request field, a new required response field, or
  an unbounded allocation requirement; or
- changing the frame delimiter, length interpretation, byte order, or
  compression/content coding without an explicit negotiated version.

The distinction between a wire-compatible change and a source-compatible
change matters. A generated client may fail to compile on a newly added enum
value even if the bytes are parseable. The compatibility gate therefore covers
both wire parsing and client behavior.

### Unknown fields and enum values

For v1, retain the tested current behavior: struct-bearing requests fail
closed, the unit `health` variant currently accepts extra fields, and known
responses ignore unknown fields. Resolve the `health` exception before calling
v1 strict or using it as a compatibility promise. For v2, response envelopes
and known response variants should ignore unknown optional fields. Request
extensions should be rejected by default unless the schema provides an
explicitly optional extension mechanism; capability negotiation must prevent
silent loss of required behavior.

Unknown operation/discriminator values are never treated as a known operation.
Return an explicit unsupported result and keep the connection usable only if
the framing/parser state is known to be intact.

V2 enum fields should use an open representation or preserve the raw value.
An unknown value must be surfaced as unknown, not silently mapped to a
meaningful default. If the value controls a side effect or a required response
decision, reject it as unsupported. This follows the [Protocol Buffers
evolution guidance](https://protobuf.dev/programming-guides/proto3/), which
documents additive fields, unknown-field preservation, reserved field numbers,
and the fact that unrecognized enum values may be represented differently by
generated languages. If Protocol Buffers is selected, never reuse removed field
numbers and use an explicit zero/unspecified enum value.

### Binary payloads

V1 keeps the additive `publish_bytes`/`message_bytes` forms established by
[ADR 0022](../decisions/0022-provisional-binary-payloads.md). They make the
binary boundary explicit and preserve text readability. V2 should carry the
logical payload as a length-delimited byte field in the negotiated envelope;
base64 may remain a JSON bridge, but it must not become the logical model.
Compression, if added, is a transport or storage content coding and must be
identified separately from payload encoding. A consumer always receives the
same logical bytes, regardless of representation.

The exact v2 schema codec remains open. Protocol Buffers is a candidate because
its numbered fields and length-delimited bytes have explicit evolution rules;
CBOR remains a candidate for a dynamic bridge. The [encoding and compression
research](../research/message-encoding-and-compression.md) records the broader
comparison. No codec or compression choice is accepted by this note.

### Request identity and outcomes

V2 should make these concepts explicit:

| Concept | Meaning | Retry rule |
| --- | --- | --- |
| `correlation_id` | Matches one response to one wire attempt | New value is valid on a retry; never implies deduplication |
| `request_id` | Stable application identity for an idempotent publish attempt | Reuse exactly when resolving an unknown publish; a mismatch must be an explicit error |
| confirmed | The broker returned the operation's success result | Do not replay unless the application intentionally requests another message |
| rejected | The broker definitely did not apply the operation | Fix the request or policy before retrying |
| retryable | The broker definitely did not apply it and a new connection/attempt is safe | Retry with the same intent; preserve request identity when applicable |
| unknown | The broker may have applied it | Reconnect and resolve by request identity or inspect state; do not blindly resend |

V2 error responses should carry an explicit outcome class in addition to a
stable machine-readable code and diagnostic message. This avoids forcing each
client implementation to infer retry safety from an ever-growing code list.
The server must never label an operation retryable when it may have crossed the
durability or application boundary. Publish batches must retain one outcome per
record and must not imply atomicity unless a separately negotiated operation
provides it.

The reference point is [Kafka's producer design](https://kafka.apache.org/41/design/design/):
it distinguishes a network error after a publish from a definitely failed
operation and provides idempotent producer sequencing so retries do not create
duplicates. Runnel's current request identity is deliberately smaller and
application-provided, so the future contract must document its scope, lifetime,
collision behavior, and whether it applies to single publishes and batch
records independently.

### Upgrade and rollback

The supported rollout shape should be server-first and additive:

1. Deploy a server that still accepts v1 and advertises v2, without making v2
   the default.
2. Deploy clients that can negotiate v2 but retain v1 fallback and preserve
   explicit outcome handling.
3. Enable v2 features only after capability and restart/unknown-outcome checks
   pass for the intended client population.
4. Retire v1 only in a separately announced breaking release after usage is
   measurable and a migration window exists.

Rollback to an older server is safe only while clients can use v1 and no
v2-only operation or required semantic has been enabled. Once v2 frames or
v2-only semantics are in flight, drain or fence those clients before rollback;
do not downgrade mid-connection. This note makes no claim that a future wire
upgrade automatically migrates retained storage, journals, snapshots, or
consumer state. Those formats need their own version and rollback policy.

## Compatibility fixtures and enforcement

The current [wire test suite](../../crates/runnel-protocol/tests/wire.rs) pins
the v1 boundary with source-level fixtures. It should remain small and
language-neutral:

- canonical request and response fixtures cover every current tag and exact
  field names, including omitted optional fields;
- fixtures cover reordered JSON members, because object order is not semantic,
  and reject duplicate-member fixtures rather than assigning them meaning;
- request fixtures reject unknown fields on struct-bearing variants, reject
  unknown tags, malformed JSON, malformed base64, and text/binary
  contradictions, while documenting the current permissive `health` unit
  variant; response fixtures verify current unknown-member and unknown-tag
  behavior;
- binary fixtures include empty bytes, NUL, non-UTF-8 bytes, standard padded
  base64, and malformed encodings;
- request-ID fixtures prove IDs survive serialization on publish forms and are
  not accidentally confused with response correlation; and
- batch fixtures preserve input order and one per-record outcome without
  asserting batch atomicity.

When v2 exists, add the following before calling it compatible:

- golden v1 and v2 frames decoded by every supported client language;
- a bidirectional old-client/new-server and new-client/old-server matrix for
  every operation and capability boundary;
- real-server tests for negotiation, no-overlap, malformed prefaces, bounded
  frames, reconnect renegotiation, and response correlation;
- injected disconnect, timeout, cancellation, and lost-response tests at
  before-write, after-write, after-apply, and after-response points, checking
  confirmed/rejected/retryable/unknown classifications and request-ID replay;
- rolling upgrade, drain, restart, and rollback tests with v1 and v2 clients;
  and
- generated-schema or independent-language checks that preserve unknown
  fields/enums and exact binary payloads.

No real-server compatibility test is added in this slice. The server has no
version negotiation or v2 framing to exercise; a proxy that merely injects an
unsupported version would test a fake runtime. The existing process-level
retry test remains the appropriate evidence for current unknown publish
outcomes. Implementing the negotiation boundary, then adding the real-server
matrix above, is a follow-up required to retire TD-003.

## Unresolved decisions

- Should v2 share the current listener with first-byte dispatch, or use a
  separate listener during the migration window?
- Is the preface plus Hello exchange preferable to a JSON Hello line, given
  that v2 needs bounded binary frames and must not be confused with v1?
- Should v2 version ranges be per operation, per connection, or both?
- Should the schema codec be Protocol Buffers, CBOR, or another bounded format?
- What exact outcome vocabulary, error codes, request-ID scope/retention, and
  batch-resolution operation are needed for non-publish requests?
- What client/server support matrix and deprecation window are sufficient to
  make rollback operationally safe?
