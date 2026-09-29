# Protocol compatibility and evolution

- Status: proposed design; not an accepted compatibility contract
- Date: 2026-09-02
- Last reviewed: 2026-09-29
- Baseline reviewed: `3d2f2a6a68ef978ed43a0735159f26db332483d9`
- Scope: public client/broker requests and responses
- Related debt: TD-003, TD-018, TD-023, TD-025, and [Make client interactions dependable and evolvable](../backlog.md#make-client-interactions-dependable-and-evolvable)
- Related evidence: [clustered outcome contract](clustered-outcome-contract.md), [application-aware retry policy](application-aware-retry-policy.md), [durability and delivery policy](durability-delivery-policy.md), [message encoding and compression research](../research/message-encoding-and-compression.md), [ADR 0022](../decisions/0022-provisional-binary-payloads.md), [ADR 0024](../decisions/0024-explicit-offset-replay-read.md), and [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)

This note records the current wire boundary and recommends a path to a
versioned client/broker contract. Observations are tied to the baseline above;
the proposed behavior is not an accepted compatibility contract. The crate's
v1 support constants are source metadata only. Nothing here closes TD-003:
runtime negotiation, interoperability, and upgrade/recovery evidence are still
required. ADR 0026 closes the engine-level portion of TD-025, but stage-aware
outcomes remain outside the provisional wire.

## Policy summary

Treat the existing line-delimited JSON mode as the provisional `v1`
implementation label, not a cross-release compatibility promise. Its current
support declaration is not sent over a connection. Continuing to accept v1
during a migration is an option to evaluate, not a commitment to support it
indefinitely.

For the first actually negotiated protocol, prefer a connection-scoped Hello
that selects one protocol version and its required capabilities before any
application request. The proposed v2 transport uses an unambiguous preface and
bounded length-delimited frames. Keep the first version request/response
sequential, matching the current persistent-client behavior; add correlation
IDs only if multiplexed or out-of-order requests are deliberately introduced.
The exact preface, schema codec, transition mode, and field layout require an
ADR and real-server tests before implementation.

The logical message remains an opaque payload, an optional UTF-8 key, a broker
offset, and delivery metadata. Wire encoding, compression, and durable storage
are separate choices. A new wire representation must not alter acknowledgement,
redelivery, ordering, durability, or the meaning of an offset.

The rollout must not treat protocol negotiation as evidence that clustered
peer RPCs or on-disk formats are compatible. Those are independent contracts.
The source-backed reference comparison and recommendation are in
[Reference designs and alternatives](#reference-designs-and-alternatives).

## Current v1: observed boundary

The [protocol types](../../crates/runnel-protocol/src/lib.rs) are Serde enums
tagged by `op` for requests and `type` for responses. The server reads one
object per TCP line and writes one response line. The reusable [client](../../crates/runnel-client/src/lib.rs)
serializes one request, waits for one response, and does not pipeline or retry
automatically. The current operation surface is:

| Direction | Tags | Compatibility-relevant fields |
| --- | --- | --- |
| Request | `create_stream`, `publish`, `publish_bytes`, `publish_batch`, `poll`, `replay`, `poll_group`, `configure_consumer`, `inspect_consumer`, `ack`, `ack_group`, `health` | UTF-8 names, keys, and consumer/member identities; text `payload`; explicit `payload_base64`; optional publish or per-record `request_id`; ordered batch outcomes; inclusive replay `offset`; durable consumer-policy settings |
| Response | `stream_created`, `published`, `publish_batch`, `message`, `message_bytes`, `replay_message`, `replay_message_bytes`, `empty`, `acknowledged`, `consumer_policy`, `health`, `error` | offsets, text or binary payload, optional group delivery fields for normal delivery, replay without delivery state, acknowledgement and consumer-policy state, current error `code` and diagnostic `message` |

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
- `payload` is UTF-8 text. `publish_bytes` and binary publish-batch records,
  plus `message_bytes` and `replay_message_bytes`, carry exact application
  bytes as standard padded base64 in `payload_base64`; the legacy text shape is
  not silently reinterpreted as base64. Base64 expansion counts against the
  current request-frame limit. The client has a local response-buffer limit,
  defaulting to `MAX_RESPONSE_BYTES`, while the server has no negotiated or
  equivalent response-size bound and serializes a response before writing it.
- `replay` is an inclusive, one-offset, read-only operation. Its successful
  response has no delivery token or attempt, and an unavailable offset returns
  `history_unavailable` rather than the ordinary `empty` poll result. It does
  not change consumer progress or delivery state.
- `request_id` is an application-provided publish identity. It is present on
  single publishes and batch records, is not echoed in the current response,
  and is not a general response correlation ID. The local and clustered
  engines scope it per stream and return the original offset when it is reused,
  even if the retry's key or payload differs; there is no producer namespace,
  request fingerprint, or retention window in the current v1 contract.
- A TCP connection can remain open for multiple requests, but the server
  handles requests serially and the reusable Rust client waits for each
  response before sending its next request. There is no v1 pipelining,
  multiplexing, or response-correlation field; response order supplies the
  association. The client discards a connection after transport/read failure,
  response-size overflow, or an unexpected typed response and callers reconnect
  explicitly.

The current [server retry test](../../crates/runnel-server/tests/client_retry.rs)
demonstrates the important outcome boundary: after a response is lost, the
client reports an unknown publish attempt, and explicitly replaying the same
publish identity returns the existing result without a duplicate. The local
and clustered engine tests also cover persistence of that identity and the
current per-stream, mismatch-ignoring behavior. A connection failure before
any request bytes are sent is retryable; a write, timeout, EOF, or cancellation
after writing may have reached the broker and is unknown. The client
intentionally leaves retry policy to the caller.

These facts describe the implementation at this baseline. They are not a
claim that arbitrary v1 clients and future servers interoperate.

### Current resource-admission boundary (TD-023)

Resource admission and protocol compatibility are separate contracts. The
server's [`ProtocolAdmission`](../../crates/runnel-server/src/protocol.rs)
configuration protects this process from bounded classes of client pressure;
the shared [`PROTOCOL_SUPPORT`](../../crates/runnel-protocol/src/lib.rs)
constant only describes what the current source was built to understand. The
listener does not send a preface or Hello, and it does not negotiate any of
these values with a client.

The resource boundary currently established by the server is:

| Resource or stage | Observed guarantee | Deliberately not guaranteed |
| --- | --- | --- |
| TCP connections | `--max-connections` is enforced with a semaphore at accept time. An over-limit client receives a best-effort `connection_limit` error and is not assigned a connection task. | No authentication, per-client identity, fairness, or TLS boundary exists. The limit is process-local and is not advertised on the broker connection. |
| Request frame | `--max-request-bytes` is validated between 1 and 64 MiB. The reader bounds retained frame bytes before JSON parsing; JSON and base64 representation bytes count toward the limit. Oversized frames receive `request_too_large` and close that connection. | No negotiated request or response size exists. The server does not promise that a client-side size setting matches its configured limit. |
| Response frame | The client bounds response buffering with its local `max_response_bytes` setting and discards the connection when that bound is exceeded. | The server currently serializes a response before writing it and has no equivalent configured response-size or pre-allocation bound. Response size is not advertised or negotiated. |
| In-flight work | `--max-in-flight-requests` is acquired only after a complete request parses and is held through engine handling, response serialization, and socket write. Saturated requests receive `request_saturated`; slow readers do not consume this permit, while slow writers intentionally do. | No queueing, priority, per-tenant quota, or admission guarantee exists for a particular operation. |
| Request time | `--request-timeout-ms` bounds incomplete frame reads and request handling, with response-write expiry tracked separately. A timeout after request bytes were sent can still be an unknown operation outcome. | The timeout is not proof that the engine did not apply a mutation, and it is not a negotiated deadline or an end-to-end latency SLO. |
| Shutdown | Idle and partial frame reads observe shutdown, and the listener drains accepted connection tasks within the server shutdown bound. | A client is not promised that an in-progress operation will be cancelled before the engine crosses its durability boundary. |

The real-process [`admission` tests](../../crates/runnel-server/tests/admission.rs)
cover configured-limit reporting, connection floods, bounded oversized frames,
partial and slow reads, slow response writers, sustained in-flight saturation,
recovery of health and durable traffic, and the corresponding low-cardinality
metrics. They do not establish behavior under sustained filesystem pressure,
host memory/CPU exhaustion, or a full multi-resource pressure matrix; those
remain part of TD-023.

The compatibility boundary at the same baseline is narrower:

- The server, client, and wire crate declare the same provisional
  `runnel-json-lines` v1 support and UTF-8/base64 payload forms. The server's
  alignment test checks these declarations, but it is a source-level check.
- A v1 connection starts sending JSON-lines immediately. There is no runtime
  protocol-name check, version negotiation, capability discovery, negotiated
  frame/response limit, or typed unsupported-version result.
- The v1 error response contains a code and diagnostic message, not an
  authoritative operation stage/outcome. The reusable client therefore keeps
  client-local classifications: `connection_limit`, `request_saturated`, and
  `stream_not_ready` are retryable; `request_timeout`, `storage_error`,
  `consumer_state_error`, `internal_error`, `cluster_error`, and
  `corrupt_record` are unknown; other broker error codes are rejected. Local
  encoding/validation failures are rejected, connection-establishment failures
  are retryable, and transport failures once a request may be writing are
  unknown. These categories are not carried by the v1 wire and are not a
  cross-language promise.
- The client can bound response buffering with `max_response_bytes`, but the
  server does not advertise or enforce that client-local value as a connection
  property. Reconnecting does not renegotiate it.

The current wire bounds are asymmetric: the server's configurable request-frame
limit is 1 through 64 MiB (including JSON/base64 representation), publish
batches are additionally limited to 1,024 records and 64 MiB of encoded
request bytes, and the client defaults its response buffer to 65 MiB. The
server serializes a response before writing and has no corresponding
configured response-frame ceiling. Connection count, in-flight work, and
request-timeout settings are local admission controls, not capabilities the
listener advertises. A negotiated design should expose only limits the client
must obey, with separate client-to-server and server-to-client encoded-frame
bounds and explicit batch limits; an advertised timeout or capacity must not
imply that resources are reserved for a request.

Consequently, a v1 client and server sharing the same Rust declaration is not
evidence of cross-release interoperability. An unknown operation currently
fails through v1 request/response parsing; it is not a capability probe with a
stable fallback contract. Future v2 work must keep these resource limits
independent from a connection-scoped version/capability handshake and add the
real-server old/new, no-overlap, reconnect, and malformed-preface tests listed
below.

The accepted [engine error classification](../decisions/0026-semantic-engine-error-classification.md)
is deliberately narrower than a future wire outcome contract. `BrokerError::kind()`
provides a backend-independent reason and `BrokerError::outcome()` provides a
conservative `Rejected`, `Retryable`, or `Unknown` engine result; successful
engine results are confirmed. The v1 server still emits only its existing
`code` and `message` fields, maps internal routing failures to `cluster_error`,
and the reusable client keeps that generic response `Unknown`. Neither an
engine outcome nor a transport error establishes a commit/apply stage for a
client.

## Proposed v2 compatibility policy

### Version and framing

The first negotiated release should select one protocol major before any
application request. A bounded preface identifies the wire family and keeps
the server from guessing a codec from arbitrary bytes; a small `Hello` then
exchanges supported major/minor ranges and the capabilities each side needs.
The server selects one common version, reports the selected capability set and
wire limits, or returns an explicit `unsupported_version` or
`unsupported_capability` refusal and closes the connection. The client must
validate that the selection is within its offer. A minor version may add only
documented compatible behavior; use a new major or operation version when
existing field or operation meaning changes. Do not negotiate each operation
separately until independent API version ranges solve a real compatibility
problem.

V2 frames should be length-delimited and bounded before allocation. The Hello
itself must also have a small fixed upper bound. Negotiate separate encoded
frame limits in each direction, plus operation-specific limits such as maximum
batch records/bytes and supported payload encodings. Keep application payload
size distinct from encoded frame size, especially if base64 or compression is
used. Advertise only limits the client must obey; admission capacity,
connection-count, and timeout settings do not reserve resources and are not
connection capabilities. Compression remains opt-in and must have bounded
decoded size and work before it is accepted.

The current Rust client keeps one TCP connection open and sends one request
then waits for one response. Preserve that ordered sequential exchange in the
first negotiated release. Response order already identifies the request, so a
`correlation_id` adds no value until pipelining, out-of-order completion, or
multiplexing is intentionally supported. If that capability is later added,
use an opaque per-attempt correlation ID and echo it in responses. It remains
separate from an application operation identity: a retry may use a new
correlation ID but must reuse the same stable identity when the broker supports
deduplication or outcome lookup. V1 `request_id` is publish-only; do not
silently broaden its scope.

Negotiation is connection-scoped. Every reconnect performs Hello again because
the peer may have been upgraded, rolled back, or may enforce different limits.
A v2 connection never changes framing midstream. Do not silently downgrade
after an application request; a client may use v1 only through an explicit
mode or a separately specified, pre-request legacy-detection rule.

### Transition alternatives

Three transition shapes remain possible:

| Alternative | Benefit | Cost and risk |
| --- | --- | --- |
| Replace provisional v1 at a documented development break | No dual parser or indefinite legacy promise; consistent with the fact that v1 has no compatibility guarantee. | Existing separately upgraded clients and broker processes stop interoperating until both are updated. |
| Dispatch v1 and v2 on the current listener using a reserved v2 preface | Keeps one configured endpoint and allows a bounded server-first migration. | A v1-only server cannot return a typed negotiation refusal; clients need a narrowly defined legacy detection or explicit v1 mode. The dispatcher and fixtures must prove the preface cannot be interpreted as a v1 application request. |
| Add a separate v2 listener | Makes protocol selection explicit and avoids first-byte dispatch ambiguity. | Adds listener configuration, ports, deployment wiring, health/admission policy, and client configuration during the transition. |

Because the repository states no v1 backward-compatibility commitment and has
no published external client matrix, do not keep a dual listener merely to
preserve an assumed promise. Prefer a deliberate protocol replacement unless
an actual independently deployed client needs overlap. If overlap is required,
use same-listener dispatch for one measured migration window; keep v1 and v2
feature sets separate and make v1 use explicit. Never infer that a listener
transition also upgrades peer RPC or durable storage.

### Reference designs and alternatives

These primary protocol documents were reviewed on 2026-09-29. They show
patterns to compare, not compatibility guarantees Runnel can inherit.

| Reference | Relevant behavior | Application to Runnel |
| --- | --- | --- |
| [Apache Kafka protocol guide](https://kafka.apache.org/43/design/protocol/) | Each request identifies an API key and version; `ApiVersions` discovers broker ranges; clients choose a common version, receive the schema for that request version, and rediscover after reconnect. A no-overlap result is explicit. | Adopt discoverable supported ranges, exact selected wire versions, an explicit no-overlap refusal, and reconnect discovery. Kafka versions requests per API because its API surface and broker routing need it; Runnel should start with one connection-scoped version and add per-operation ranges only when independently evolving operations justify the larger test matrix. |
| [NATS client protocol](https://docs.nats.io/reference/protocols/client) | The server sends an initial JSON `INFO` containing feature flags, protocol level, and `max_payload`; later `INFO` may update connection-visible topology. `PUB` and `HPUB` carry byte counts before opaque payload bytes. | Adopt inspectable capability/limit discovery and count payloads in bytes. NATS is not mutual version negotiation: its server-first capability message and asynchronous topology updates solve different needs. Runnel currently has no client handshake or topology discovery, so these should not be implied by copying the `INFO` shape. |
| [Protocol Buffers v3 evolution guidance](https://protobuf.dev/programming-guides/proto3/) | Additive fields can be wire-safe when old readers ignore them; binary parsers preserve unknown fields, but conversions through JSON or field-by-field copying can drop them. Unknown enum representation varies by language, and field numbers must not be reused. | If a tagged binary schema is selected, reserve removed tags, define presence/defaults, keep unknown values observable, and test each supported generated client. Protobuf guidance concerns schema evolution, not transport negotiation, outcome semantics, or the choice of Runnel's frame format. |

The existing base64 JSON bridge is easiest to inspect and use from shell
clients, but expands opaque bytes and charges the expanded representation
against the request bound. A tagged binary envelope can represent bytes without
base64 and gives generated clients a formal schema, while increasing schema,
tooling, and unknown-field behavior commitments. CBOR can represent bytes with
less schema-generation overhead, but still needs an independently documented
evolution policy. The [encoding research](../research/message-encoding-and-compression.md)
contains the broader measurements and tradeoffs; this proposal does not select
Protobuf, CBOR, or a custom codec. The protocol and logical payload contract
can be specified before that codec choice is made.

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
For a well-formed v2 envelope, return a stable `unsupported_operation` result;
keep the connection usable only if the frame boundary and parser state remain
known. Unsupported protocol versions or required capabilities are preflight
refusals: no application operation has been attempted, so they need no
operation outcome. A malformed preface, impossible frame length, or truncated
frame may not have enough structure for a typed response and should close the
connection.

V2 enum fields should use an open representation or preserve the raw value.
An unknown value must be surfaced as unknown, not silently mapped to a
meaningful default. If the value controls a side effect or a required response
decision, reject it as unsupported. This follows the [Protocol Buffers
evolution guidance](https://protobuf.dev/programming-guides/proto3/), which
documents additive fields, unknown-field preservation, reserved field numbers,
and the fact that unrecognized enum values may be represented differently by
generated languages. If Protobuf is selected, never reuse removed field
numbers, use an explicit zero/unspecified enum value, and make the client API
preserve an unrecognized value rather than selecting a semantic default.

### Negotiation refusal and operation errors

Keep connection negotiation errors separate from errors for an attempted
operation. A version or required-capability refusal happens before application
traffic and closes the connection; a well-formed unsupported operation can be
rejected while keeping the connection only when its framing remains
synchronized. Invalid framing and oversized frames close the connection because
the next frame boundary cannot safely be assumed. These are candidate v2
semantics, not current v1 responses.

For an application error, v2 should carry both a stable machine-readable code
and an explicit outcome class. `Rejected` and `Retryable` are valid only when
the broker can establish that the operation did not apply. A timeout after a
request may have crossed the durability boundary is `Unknown`, even if the
server knows which stage timed out. Report a stage only when it is authoritative
and useful to clients. The code explains the reason; the outcome class explains
safe retry behavior. Clients should not derive retry safety from a growing list
of error codes.

### Binary payloads

V1 keeps the additive `publish_bytes`/`message_bytes` forms established by
[ADR 0022](../decisions/0022-provisional-binary-payloads.md). They make the
binary boundary explicit and preserve text readability. The additive publish
batch and replay shapes are also part of the current v1 boundary; replay is a
read-only offset operation as accepted by [ADR 0024](../decisions/0024-explicit-offset-replay-read.md).
V2 should carry the logical payload as a length-delimited byte field in the
negotiated envelope; base64 may remain a JSON bridge, but it must not become
the logical model.
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
| `correlation_id` | Optional future field that matches one response to one wire attempt if multiplexing is added | New value is valid on a retry; never implies deduplication |
| Candidate `operation_id` | A stable application identity only for operations with designed durable deduplication or outcome lookup | If supported, reuse exactly to resolve an unknown operation; a mismatch must be an explicit error |
| v1 `request_id` | Current publish-only identity, scoped per stream and without a stored fingerprint | Reuse resolves the stored offset today; v2 must not assume this behavior is sufficient for generic operations |
| confirmed | The broker returned the operation's success result | Do not replay unless the application intentionally requests another message |
| rejected | The broker definitely did not apply the operation | Fix the request or policy before retrying |
| retryable | The broker definitely did not apply it and a new connection/attempt is safe | Retry with the same intent; preserve request identity when applicable |
| unknown | The broker may have applied it | Reconnect and resolve by request identity or inspect state; do not blindly resend |

Do not add a generic `operation_id` until its storage and lifecycle semantics
are designed. The current `request_id` is publish-only, per-stream, persists
with the record, and does not fingerprint key or payload; reusing it with
different content returns the earlier offset. A generic identity would need a
namespace, retention/lifetime bound, collision and content-mismatch behavior,
forwarding scope, and crash-recovery contract for each supported operation.
Where no durable deduplication or lookup exists, return `Unknown` and direct
the caller to inspect state or apply its own reconciliation; an ID field alone
does not make retry safe. Batch results must retain one outcome per record and
must not imply batch atomicity unless a separately designed operation provides
it.

The reference point is [Kafka's producer design](https://kafka.apache.org/43/design/design/):
it distinguishes uncertain publish attempts from definitely failed requests
and provides producer sequencing for idempotent retries. That mechanism has
broker-side producer state and sequencing semantics Runnel does not have.
Borrow the explicit outcome distinction, not the assumption that adding a
generic ID field creates idempotence.

### Upgrade and rollback

There is no observed independently deployed client population or published v1
support matrix yet. Thus a server-first rollout is a conditional migration
plan, not a current support commitment. If separate client and broker releases
need overlap, use this staged path:

1. Publish the exact v1/v2 support matrix and decide whether a bounded dual
   listener is needed. If replacing v1, coordinate broker and Rust-client
   rollout at a documented breaking boundary.
2. Where overlap is needed, upgrade every broker node to a server that accepts
   v1 and v2 before v2 is enabled in clients. Keep new clients in explicit v1
   mode during a mixed-node upgrade; a per-node Hello does not prove every
   cluster node can handle a v2-only capability.
3. Upgrade the supported Rust client and CLI to negotiate v2. Enable v2 only
   after old/new fixtures, real-server restart and unknown-outcome checks, and
   a release-specific client population check pass. Keep v1 use explicit while
   the migration window is open; do not retry a failed v2 handshake as v1 on
   timeout, EOF, or malformed data.
4. Remove v1 only at a separately documented breaking boundary after actual
   usage and migration needs are known. If there is no deployed v1 population,
   do not add an indefinite deprecation window by assumption.

| Client and server | Required behavior during an explicitly supported transition |
| --- | --- |
| v1 client to v1-only server | Current provisional wire behavior only; no cross-release promise is established. |
| v1 client to dual v1/v2 server | Use the v1 path and v1 schemas; do not infer support for v2 operations from the server process being newer. |
| v2-required client to v1-only server | Fail before application traffic. A v1-only listener may answer a v2 preface with generic `invalid_request` or close it; the v2 client must treat this as failed negotiation, not a broker operation outcome. |
| v2 client to v2 server with overlap | Use the exact selected version and capability subset. A required capability with no overlap is a typed preflight refusal; optional unsupported behavior needs an explicit safe fallback. |
| mixed-version cluster nodes | Keep v2-only operations disabled until every node that can accept or forward them supports the required behavior. External protocol agreement does not imply peer-protocol or storage compatibility. |

An explicit client-configured v1 mode can serve as a temporary fallback if the
operation is representable without losing semantics. Automatic fallback is
only safe before application requests and requires a defined legacy-detection
response; never infer compatibility from a timeout, EOF, or arbitrary parse
error. Rollback to an older server is safe only when v2-only operations and
required semantics have not been used and every participant understands the
written state. Drain/fence v2 connections before a rollback and renegotiate on
reconnect. A wire downgrade does not migrate retained storage, journals,
snapshots, consumer state, or engine selection. The single-node-to-cluster
migration is a separate logical data-movement problem.

## Compatibility fixtures and enforcement

The current [wire test suite](../../crates/runnel-protocol/tests/wire.rs)
exercises Rust serialization/deserialization behavior; it is not a
cross-language golden-fixture suite. v1 fixtures pin observed behavior for
migration analysis only and do not grant a support promise. Keep fixtures
language-neutral so future clients can consume supported protocol releases:

- canonical request and response fixtures should cover every current tag and
  exact field names, including omitted optional fields. The current Rust suite
  does not explicitly exercise the `inspect_consumer` request variant, so it
  is not yet exhaustive even within this language;
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
  not accidentally confused with response correlation; replay fixtures prove
  the read-only response has no delivery token or attempt; and
- batch fixtures preserve input order and one per-record outcome without
  asserting batch atomicity.

When v2 exists, add the following before calling it compatible:

- golden frames for each protocol release still in its support window decoded
  by every supported client language; include v1 only if a bounded v1
  transition is actually selected;
- a bidirectional old-client/new-server and new-client/old-server matrix for
  every supported transition mode, operation, and capability boundary,
  including the v1-only-server refusal path and proof that no application
  request was sent before negotiation completed;
- real-server tests for negotiation, no-overlap, malformed prefaces, bounded
  frames, reconnect renegotiation, response ordering, and typed refusal;
  add response-correlation tests only if a multiplexing capability is accepted;
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
outcomes, while the replay tests establish the additive read-only operation
and the clustered outcome tests establish only the engine-level classification.
Implementing the negotiation boundary, then adding the real-server matrix
above, is a follow-up required to retire TD-003.

The current [real-server retry test](../../crates/runnel-server/tests/client_retry.rs)
proves that a lost publish response is classified as `Unknown` and that the
same v1 `request_id` can resolve it without appending a duplicate. It does not
prove generic operation-ID behavior, cross-version negotiation, or that a v1
error code carries a backend-independent apply stage. Retain that distinction
in fixture names and compatibility reports.

## Unresolved decisions

- What exact preface and bounded Hello encoding identify the protocol, and is a
  temporary same-listener v1 dispatcher needed for an actual deployed-client
  population?
- Should a later protocol introduce per-operation version ranges, or remain
  connection-scoped if operation capabilities are sufficient?
- Should the schema codec be Protocol Buffers, CBOR, or another bounded format?
- What exact outcome vocabulary and machine-readable negotiation/error codes
  make refusals distinct from operation failures across client languages?
- What identity, retention, mismatch, and resolution contract is justified for
  operations beyond publish, including per-record batch retries?
- What resource limits are safe to renegotiate only on reconnect, and which
  would ever need an in-band update?
- Which external client languages and deployment patterns justify a published
  support matrix and a v1 deprecation window?

## Disposition and near-term implementability

The compatibility finding is actionable soon, but implementation should follow
an accepted ADR rather than adopting this proposal by implication. The existing
protocol crate, persistent Rust client, listener, and real-process test harness
provide a bounded starting point for a first negotiated Rust client/server
slice: one version selection per connection, explicit capability and directional
frame limits, sequential requests, typed preflight refusal, and an outcome
class on operation errors. The project has not chosen a maintained non-Rust
client language, so cross-language fixtures should be required before claiming
interoperability but need not block the initial Rust runtime experiment.

Keep codec selection, generic operation deduplication, compression,
multiplexing, and indefinite v1 support outside that first slice until their
user need and lifecycle costs are established. Before coding, decide the exact
handshake bytes, compatibility/replace strategy, stable error model, and how
v1 is detected or selected; then record the accepted consequence in an ADR and
build old/new client-server fixtures plus real-server negotiation, reconnect,
refusal, and ambiguous-outcome coverage. The existing client/backlog outcome
and TD-003 already track runtime compatibility and interoperability; this
research update does not warrant a duplicate backlog or tech-debt item. Update
those records when implementation changes their current progress or retirement
criteria.
