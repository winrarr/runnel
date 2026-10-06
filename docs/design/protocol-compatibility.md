# Protocol compatibility and evolution

- Status: accepted protocol contract; runtime implementation and compatibility claims remain open
- Date: 2026-09-02
- Last reviewed: 2026-10-06
- Baseline reviewed: `c3a894b6d88a40245c1116e2c5006b94f5573aee`
- Scope: public client/broker requests and responses
- Related debt: TD-003, TD-018, TD-023, TD-025, and [Make client interactions dependable and evolvable](../backlog.md#make-client-interactions-dependable-and-evolvable)
- Related evidence: [clustered outcome contract](clustered-outcome-contract.md), [application-aware retry policy](application-aware-retry-policy.md), [durability and delivery policy](durability-delivery-policy.md), [message encoding and compression research](../research/message-encoding-and-compression.md), [ADR 0022](../decisions/0022-provisional-binary-payloads.md), [ADR 0024](../decisions/0024-explicit-offset-replay-read.md), [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md)

This design records observed v1 behavior and the accepted v2 client/broker
contract from [ADR 0031](../decisions/0031-protocol-v2-contract.md).
Observations are tied to the baseline above. The v2 contract is not implemented
and does not establish a released support window or cross-release guarantee.
The crate's v1 support constants remain source metadata only. TD-003 stays open
until runtime negotiation, interoperability, and compatibility evidence pass.
ADR 0026 closes the engine-level portion of TD-025; ADR 0031 accepts the future
v2 stage-aware outcome contract, which is not present in provisional v1.

## Policy summary

Treat the existing line-delimited JSON mode as the provisional v1
implementation label, not a cross-release compatibility promise. Its support
declaration is not sent over a connection.

[ADR 0031](../decisions/0031-protocol-v2-contract.md) accepts the first
negotiated v2 contract: a fixed RNLN bootstrap preface, bounded Hello with
major/minor and required-capability selection, Protobuf v3 envelopes,
bounded length-prefixed frames, and sequential request/response exchange.
Negotiation and effective directional limits are scoped to one connection and
are repeated after reconnect. V2 has a typed refusal before application
traffic when a valid Hello has no version or required-capability overlap. It
does not silently fall back to v1.

V2 carries opaque message bytes directly and has explicit confirmed, rejected,
retryable, and unknown outcomes with topology-neutral processing stages. The
outcome and stage contract is accepted, but no v2 runtime or cross-language
support matrix exists yet. Compatibility and rollout claims wait for the
real-server and generated-client evidence in the accepted decision.

The logical payload, public wire encoding, peer transport, and durable storage
remain separate boundaries. Public protocol negotiation does not establish
peer RPC or disk-format compatibility. The reference comparison is in
[Reference comparison and rationale](#reference-comparison-and-rationale).
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

The consumer-policy operations are part of the current source-level v1
inventory, following [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md):
`configure_consumer` takes a stream, consumer, acknowledgement timeout, and
optional maximum delivery-attempt count; `inspect_consumer` takes the stream
and consumer. Both return `consumer_policy` with a policy `version`, a
`configured` flag, the effective acknowledgement timeout, and the optional
attempt limit. The policy version is a consumer-state version, not a wire
protocol version. Unconfigured consumers report broker-wide fallback settings
with version zero and `configured: false`; explicit policies start at version
one, same-value configuration is idempotent, and changed values advance the
version. Both engines persist explicit policies. A configured timeout may be
zero through seven days; a present attempt limit must be positive. Within an
explicit policy, `max_delivery_attempts: null` (or an omitted input field)
means no per-consumer attempt ceiling; broker-wide fallback applies only while
the consumer has no configured policy.

These operations were added to the provisional v1 enum without runtime
capability discovery. A client and server that both declare v1 therefore do
not thereby prove that an older v1 server implements `configure_consumer` or
`inspect_consumer`; an unrecognized operation is currently a generic
`invalid_request` parsing failure, not a stable unsupported-capability result.
The declaration and current operation inventory describe this source tree,
not an older-release compatibility guarantee.

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

The [server retry test](../../crates/runnel-server/tests/client_retry.rs)
demonstrates the single-publish outcome boundary: after a response is lost,
the client reports an unknown attempt, and replaying the same publish identity
returns the existing result without a duplicate. Real-process typed-client
tests now cover the corresponding batch boundary: dropping the response or
withholding a complete successful response past the client timeout marks every
record unknown, while retrying the same per-record IDs after reconnect returns
the original offsets. A three-process test also withholds a successful batch
response, changes leaders, and confirms stable-ID retry against the survivor
does not duplicate records. These tests strengthen current-v1 outcome evidence;
they do not establish cross-release compatibility. The local and clustered
engine tests cover persistence of publish identity and its current per-stream,
mismatch-ignoring behavior. A connection failure before any request bytes are
sent is retryable; a write, timeout, EOF, or cancellation after writing may
have reached the broker and is unknown. The client intentionally leaves retry
policy to the caller.

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
| Request frame | `--max-request-bytes` defaults to 1 MiB and is validated between 1 byte and 64 MiB. The reader bounds the JSON-line body before parsing, retaining at most the configured body limit plus its LF terminator; JSON syntax and base64 representation bytes count toward the body limit. Oversized frames receive `request_too_large` and close that connection. | No negotiated request or response size exists. The server does not promise that a client-side size setting matches its configured limit. The reusable client has no general request-size setting and does not preflight ordinary publishes against the server's configured bound. |
| Publish batch | The typed client rejects an empty batch, more than 1,024 records, or a serialized JSON body above 64 MiB before sending. The server rejects empty or over-1,024-record batches after parsing; its general request-frame limit, capped at 64 MiB, bounds the encoded JSON/base64 body. | The server has no separate batch-byte check beyond its request-frame bound, and raw `Request` callers do not receive the typed client's batch preflight. A complete response preserves per-record order but does not promise atomicity. |
| Response frame | The client bounds response buffering with its local `max_response_bytes` setting, defaulting to 65 MiB including the optional line terminator, and discards the connection when that bound is exceeded. | The server currently serializes a complete response before writing it and has no equivalent configured response-size or pre-allocation bound. Response size is not advertised or negotiated. |
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

The current wire bounds are asymmetric: the server defaults to a 1 MiB
request-body bound and permits a configured maximum of 64 MiB; typed batch
publishes additionally enforce 1,024 records and a 64 MiB serialized JSON
body. The batch byte ceiling is also the server's maximum general request
body, rather than a second batch-only server check. The client defaults its
response buffer to 65 MiB, while the server serializes responses without a
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

## Accepted v2 contract

ADR 0031 selects a first negotiated public protocol. The decisions below are
accepted design constraints for its future runtime; they are not descriptions
of the current listener. The current JSON-lines v1 remains provisional and
carries no cross-release compatibility promise.

### Bootstrap, negotiation, and reconnect

A v2 client begins every TCP connection with this exact eight-byte preface:

| Bytes | Meaning |
| --- | --- |
| `52 4e 4c 4e` | ASCII `RNLN`, the Runnel negotiation family |
| `01` | Bootstrap revision 1; this identifies the Hello framing, not application protocol major 1 |
| `00 00 00` | Reserved; senders write zero and receivers reject nonzero values |

The client then sends one bounded Hello frame. The Hello and Hello reply use a
four-byte unsigned big-endian body length followed by a Protocol Buffers v3
message. The Hello body may be at most 16 KiB. A zero length, truncated Hello,
invalid Protobuf message, unsupported bootstrap revision, or nonzero reserved
byte closes the connection. A peer that has parsed a valid Hello sends either
a Hello reply or a typed refusal and then closes after a refusal.

Each Hello lists inclusive minimum and maximum minor versions for each
supported major, the client's offered and required capabilities, and its
maximum outbound and inbound application-frame bodies. Each positive major
appears once, and each inclusive minor range has a minimum no greater than its
maximum. The server selects the highest common major and then the highest
common minor within that major. A
required capability must be offered by the client and supported by the
server. Capability names are case-sensitive ASCII identifiers matching
`[a-z][a-z0-9_]{0,63}`; offered and required capabilities are duplicate-free
sets, and the required set is a subset of the offered set. A parseable Hello
with invalid ranges or capability sets receives `invalid_hello` and closes.
The reply lists the selected version, the selected subset of offered
capabilities, the server's maximum inbound and outbound body sizes, and the
effective frame limits. For client-to-server traffic, the effective limit is
the minimum of the client's outbound ceiling and the server's inbound ceiling;
for server-to-client traffic, it is the minimum of the client's inbound ceiling
and the server's outbound ceiling. The client verifies both exact minima, that
the version is in its offer, all required capabilities were selected, and no
unoffered capability was selected. A missing version or unsupported required
capability returns a typed `unsupported_version` or
`unsupported_capability` refusal.
A limit below 1 KiB returns `limit_too_small`; a ceiling above its directional
hard maximum returns `limit_too_large`. A client that receives an effective
limit other than the computed minimum treats it as a protocol violation and
closes; it also rejects server-advertised endpoint limits outside the
directional bounds. A refusal means no application operation was attempted. The client
sends no application frame until it has received and validated a successful
Hello reply. The server does not process application traffic until it has sent
a successful Hello reply.

Unknown optional capabilities are ignored. A client must require every
capability needed to interpret or perform its request; it must not send the
request if that capability was not selected. Core v2 behavior is defined by
the selected version and does not need
a capability flag. A future `consume_batch` capability can gate its operation;
the client must require that capability in Hello before sending any operation
it defines, and the server refuses an unsupported requirement before
application traffic. This does not fix the operation or field names in this
decision. The first v2 release uses one
connection-scoped version and advertises exactly 2.0 on both client and server;
later 2.x minors are offered only after their compatible behavior is
implemented. V2 does not negotiate independent operation versions.

Negotiation is repeated after every reconnect. A connection keeps the same
selected version, capabilities, and limits until it closes. A server restart,
leader change, or new connection never inherits the previous connection's
selection. If a reconnect cannot negotiate, the client reports the handshake
failure and does not retry the application request as v1.

### Application frames, schema, and limits

V2 application frames use a four-byte unsigned big-endian length followed by
one Protobuf v3 envelope. The declared length counts only the encoded Protobuf
body, not the four-byte prefix. Zero-length, truncated, or over-limit frames
are rejected and the connection is closed; implementations validate the
length before allocating the body and do not scan for a later frame boundary.
The client envelope is a top-level Protobuf oneof whose field tags are reserved
for operation variants; the initial envelope has no optional metadata fields.
The server envelope similarly contains one operation reply. This makes an
unknown top-level client tag an unknown operation rather than an ambiguous
extension.

The server and client exchange exactly one application request and its
response at a time. No pipelining, multiplexing, out-of-order completion, or
correlation ID is part of v2. Response order identifies the request.

The initial v2 envelope carries message payloads as opaque `bytes`; text and
binary client helpers use the same wire representation. Stream, consumer,
member, and optional key fields remain UTF-8 text. There is no compression in
the initial capability set. A future compression capability must separately
bound encoded bytes, decoded bytes, and decoding work, and preserve the same
logical payload.

The initial hard body limits are asymmetric: 64 MiB client-to-server and 65
MiB server-to-client. The server's default accepted request body is 1 MiB and
may be configured up to the 64 MiB hard maximum. The server's response body
limit defaults to 65 MiB and may be configured lower, never higher. Each client
advertises its send and receive ceilings. The negotiated client-to-server
limit is the lower of the client's send ceiling and the server's configured
receive limit; the negotiated server-to-client limit is the lower of the
client's receive ceiling and the server's configured send limit. Both values
must be at least 1 KiB and at most their directional hard maximum, or the
server refuses Hello with `limit_too_small`; an over-maximum ceiling receives
`limit_too_large`. Both are per connection and count
encoded body bytes, including Protobuf metadata and byte fields, but excluding
the four-byte prefix. The 16 KiB Hello cap is separate.

The existing publish-batch ceiling of 1,024 records remains a v2 operation
limit; the encoded request body also remains subject to the negotiated and hard
request limits. Every response-producing operation must check that its result
fits the negotiated response limit before it crosses a state-changing
boundary. If it cannot fit, it returns a bounded `response_too_large` rejection
with evidence that no effect was applied. It must never assign a delivery,
advance consumer state, or publish a record and then silently truncate the
response. Connection count, in-flight work, request duration, and storage
admission remain local server controls; Hello does not reserve those resources
or advertise them as capabilities.

For Protobuf evolution, added fields in a later minor are optional and have a
safe omission behavior. A sender relies on a field only when the selected
minor or capability defines it. Existing field numbers, wire types, presence,
defaults, and meanings do not change within a major. Removed field numbers and
enum numbers are reserved and never reused. Peers do not act on fields outside
the selected schema or capability. Protobuf binary APIs can preserve unknown
fields, but conversion or reconstruction may discard them, so a sender cannot
rely on an older peer applying or round-tripping an unknown request field.
Unknown operation variants are rejected as `unsupported_operation` while keeping the connection open only
when the frame remains synchronized. An unknown required response outcome or
stage cannot be given a safe meaning: the client reports an unknown attempt
and closes the connection. Machine error codes match `[a-z][a-z0-9_]{0,63}` and do not determine retry
safety; unknown codes remain diagnostic values. Optional human-readable
diagnostic text is capped at 512 UTF-8 bytes.
No field addition may silently change a required behavior.

The Protobuf schema contains a client Hello, server Hello reply/refusal, client
application envelope, server application envelope, and operation-specific
request/result messages. Every successful or failed application result
contains outcome and stage; an error additionally contains its stable code and
diagnostic, while a success contains the typed operation result. A batch result
carries item outcomes and does not infer atomicity from its outer envelope.
Field numbers are allocated in the checked-in Protobuf schema when runtime
implementation begins and then follow the no-reuse rule above; their allocation
is a mechanical schema task, not an unresolved compatibility policy.

### Outcomes and processing stages

Every server application reply carries an authoritative outcome and stage,
including successful replies. The error code explains the reason; callers use
the outcome for safety and never derive it from the code. Outcome describes
what the caller may safely conclude about this attempt:

| Outcome | Contract |
| --- | --- |
| `confirmed` | The operation reached its documented success point and the reply contains its result. A state-changing success has reached the durable stage; a read-only success has completed. |
| `rejected` | The broker has affirmative evidence that the requested application effect did not occur. The intent or its current precondition must change before retry. |
| `retryable` | The broker has affirmative evidence that no effect occurred and the same intent can safely be attempted later. This may follow engine execution only when the engine proves no proposal or effect occurred; caller policy determines when. |
| `unknown` | The broker cannot establish whether the effect occurred or the client cannot establish which result was produced. Do not treat the request as unapplied. |

Stage is the furthest broker-side point it can establish, not a synonym for
effect status and not a statement that later stages were not reached:

| Stage | Meaning |
| --- | --- |
| `received` | The complete application frame was decoded as a request. |
| `validated` | Request and operation validation completed; engine execution has not begun. |
| `execution_started` | Engine processing began; an effect may or may not have occurred. |
| `durable` | The requested state-changing effect reached its documented durability point. For the current local engine this is its applicable durable append or consumer-state sync; for the clustered engine it is quorum commit plus durable state-machine application, as defined by [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md). A stream creation is not confirmed until activation/reconciliation is complete. A durably processed no-effect rejection is not reported as a durable effect. |
| `completed` | A read-only operation completed against its documented read view. |
| `unknown` | The server cannot establish the furthest stage. |

A `retryable` reply requires proof that no effect occurred and the same intent
is safe to attempt later. It may report `received` or `validated`; it may report
`execution_started` only when the engine returned an authoritative retryable
classification proving no proposal or effect. `rejected` also requires proof of
no requested effect; it may report `execution_started` only when the engine
returned a definitive no-effect result, such as a stale delivery fence. If an
operation may have crossed its effect boundary and there is no definitive
terminal result, the outcome is `unknown`, even if the server knows it started
processing. Successful state changes require `durable`; successful reads
require `completed`. Stage alone never lets a client infer non-application.

There is intentionally no `response_written` stage. A reply cannot report its
own complete delivery; a client that decodes the entire valid reply has direct
evidence of that receipt. If the response write fails or the connection drops,
the client has no reply and treats the attempt as unknown once it started
writing the application frame. A connection failure before an application
frame write is attempted is a client-side pre-send failure, not a broker stage
or a server response. V2 clients do not automatically replay operations.

A scalar operation reply or batch-level error carries one outcome and stage. A
completed batch response has no aggregate outcome; it contains ordered item
results, each with its own outcome and stage. Operation-specific result detail
(for example, whether an acknowledgement newly confirmed or was already
confirmed) remains separate from the four safety outcomes. If no complete
batch response is received, unresolved items are unknown. An envelope-level
response never implies batch atomicity. This leaves future consume-batch naming
and fields to its own accepted contract while allowing Hello to require its
capability.

### Publish identity and mismatch behavior

The initial v2 stable request identity remains publish-only. Each single publish
or publish-batch record may carry a request ID, scoped to one stream and unique
across clients that publish to that stream. IDs contain 1 to 1,024 valid
UTF-8 bytes and compare by exact bytes without Unicode normalization. The
broker forwards the ID unchanged and retains it with its original record for
at least that record's retention lifetime. The initial implementation has no
message-retention policy, so it has no independent ID expiry window. A future
retention policy must not expire an ID while retaining the record it
identifies; the replay safety guarantee ends when that record and its identity
are both eligible for removal.

For v2, the fingerprint is the stream, key presence and exact UTF-8 key bytes,
and exact logical payload bytes. The server-assigned publish timestamp and the
request ID itself are excluded. Repeating a request ID with the same
fingerprint returns the original receipt without appending another record.
Reusing it with a different fingerprint returns `request_id_conflict`,
`rejected` at `execution_started`, and definitive no-effect evidence; the
original record is unchanged. This deliberately changes the current
provisional v1 behavior, which returns the original offset without comparing
key or payload. [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md)
continues to describe current v1 and storage identity behavior; ADR 0031
supersedes that mismatch rule for negotiated v2. The implementation must
compare intent in both local and clustered engines before v2 is claimed as
supported.

A batch has no atomicity by virtue of using request IDs. Its records retain
independent IDs and outcomes. No general operation ID or response correlation
ID is introduced for poll, acknowledgement, create, or other operations. If an
operation without a stable identity has an unknown outcome, the client must
inspect application state or make an explicit duplicate-versus-loss decision;
the library does not invent retry safety.

### Mismatch, v1 boundary, and rollout

The compatibility rules are:

| Peer or condition | Required behavior |
| --- | --- |
| Parseable Hello with invalid or duplicate major/minor ranges, malformed or duplicate capability names, or a required capability absent from the offer | Server returns `invalid_hello` and closes without processing application traffic. |
| v2 peers with no common major/minor | Server sends typed `unsupported_version` refusal after parsing the valid v2 Hello, then closes. No application operation was attempted. |
| A required capability is not supported | Server sends typed `unsupported_capability` refusal and closes before application traffic. The client does not silently drop the requirement. |
| Selected version, capability, or limit is outside the client offer | Client treats the reply as a protocol violation, sends no application request, and closes. |
| Unknown bootstrap revision, malformed preface/Hello, invalid or oversized length, or truncated frame | The receiver closes. It sends a typed refusal only when it can safely parse the valid v2 Hello; it does not guess or resynchronize. |
| v1 JSON-lines client connects to a v2-only listener | The listener rejects the non-v2 preface and closes. It does not parse the bytes as a v1 request or promise a v1-readable refusal. |
| v2 client connects to a v1-only listener | The v2 client waits only within its handshake deadline. EOF, timeout, or a non-v2 response is a handshake failure; it sends no application request and never retries as v1. |
| Well-framed unknown operation | Server returns `unsupported_operation` with a definitive rejected outcome and keeps the connection only if the next frame boundary is known. |
| Reconnect after peer restart, leader change, or failure | Client opens a new connection and repeats the complete preface and Hello. No old selection or limits carry over. |

The initial v2 deployment replaces provisional v1 at one coordinated breaking
release boundary for the server, reusable Rust client, and CLI. The v2 listener
is v2-only; there is no dual parser, second listener, implicit v1 mode, or
migration window in this decision because the repository has no published v1
support matrix or known independently deployed client population. If later
evidence justifies overlap, a new decision must define its duration, client
selection, compatibility tests, and removal point. It must not add silent
downgrade.

The protocol decision does not promise mixed-version broker-cluster upgrades,
internal peer-protocol compatibility, or storage/disk-format compatibility.
A public connection's Hello says nothing about whether another node can accept
or forward its operation. This ADR selects no rolling-upgrade sequence or
rollback behavior; those require separate peer and storage evidence. It also
adds no TLS, authentication, or other security contract.

### Reference comparison and rationale

The primary references below inform the selected boundaries; they do not
transfer their compatibility promises or application semantics to Runnel.

| Reference | Sourced behavior | Runnel decision |
| --- | --- | --- |
| [Apache Kafka protocol guide](https://kafka.apache.org/43/design/protocol/) | ApiVersions reports supported API versions, the client selects an overlap, and the client repeats discovery after reconnect. Kafka versions each API independently. | Select one connection-scoped major/minor and rediscover on every connection. Runnel does not need per-operation ranges before independent API evolution justifies their matrix. |
| [NATS client protocol](https://docs.nats.io/reference/protocols/client) | The server sends an initial INFO with protocol/features and a maximum payload; later INFO can update connection-visible state. | A bounded capability/limit exchange is useful, but Runnel chooses mutual Hello negotiation before requests. It does not inherit NATS topology updates or server-first transition behavior. |
| [PostgreSQL protocol overview](https://www.postgresql.org/docs/current/protocol-overview.html) | Startup uses a versioned startup packet before ordinary traffic and can refuse unsupported startup modes. | Keep handshake failures separate from application outcomes and do not begin operations before negotiation completes. PostgreSQL's authentication and startup model are not adopted. |
| [RFC 9293, TCP](https://www.rfc-editor.org/rfc/rfc9293.html) | TCP supplies an ordered byte stream; application writes and TCP segments do not define message boundaries. | Use an explicit bounded length prefix and validate it before allocation. |
| [Protocol Buffers v3 guide](https://protobuf.dev/programming-guides/proto3/) and [encoding guide](https://protobuf.dev/programming-guides/encoding/) | Numbered fields and length-delimited bytes support direct binary values and documented schema-evolution rules; field numbers must not be reused, and parser unknown-field behavior must be considered. | Choose Protobuf v3 for the initial typed public schema, with reserved removed tags, explicit presence/default rules, capability gates, and language-neutral golden frames. Its generated-code/tooling cost is accepted for direct bytes and formal cross-language schema; no language support is claimed until independently verified. |

Length-delimited JSON would reuse today's Serde/tooling and remain easy to inspect,
but it retains base64 expansion for opaque payloads and has no typed field-number
policy. CBOR represents bytes directly with less schema-generation overhead,
but would leave more Runnel-specific schema and evolution rules to define.
Protobuf's extra generated-schema and unknown-field discipline is accepted
because this is the future language-client boundary, not a shell-only
transport. Compression and per-operation versioning are deferred until
capability needs and workload evidence justify them.

### Implementation and compatibility gates

This is a design-only decision; no runtime negotiation or Protobuf codec is
implemented at the reviewed baseline. Before describing v2 as supported, the
implementation must provide:

- checked preface, Hello, no-overlap, required-capability refusal, client
  selection validation, reconnect renegotiation, and every mismatch row above
  in tests that start the real server;
- golden Protobuf frames for all supported operations and a generated client
  plus an independent decoder, preserving opaque bytes and unknown optional
  fields/enums according to this policy;
- request and response allocation bounds checked before allocation, with tests
  at each directional limit, on smaller negotiated limits, malformed/truncated
  frames, and response-too-large rejection before state changes;
- real-process local and clustered outcome/stage coverage for confirmed,
  rejected, retryable, and unknown outcomes, including no-effect-before-engine,
  definitive post-entry rejection, ambiguous timeouts, response loss after
  durable apply, reconnect, and safe stable-ID resolution;
- changed-ID-fingerprint rejection and same-fingerprint resolution across
  restart and leader change, without duplicate records; and
- release evidence for the coordinated v1 break and explicit statements that
  peer and disk compatibility are not covered.

The current Rust wire fixtures and real-process tests documented above remain
v1 evidence only. A future consume-batch feature or any other optional behavior
must be withheld unless Hello selected its required capability. Until these
gates pass, TD-003 and TD-025 remain open for implementation/evidence, and no
cross-release or cross-language compatibility claim is authorized.
