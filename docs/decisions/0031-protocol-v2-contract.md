# ADR 0031: Define the first negotiated public protocol contract

- Status: accepted
- Date: 2026-10-06
- Baseline: `c3a894b6d88a40245c1116e2c5006b94f5573aee`
- Primary evidence class: design/research; secondary: compatibility correctness
- Related: [TD-003](../tech-debt.md#td-003-provisional-json-lines-protocol-and-limited-payload-compatibility), [TD-025](../tech-debt.md#td-025-shared-engine-errors-expose-implementation-specific-failure-details), [client interactions backlog](../backlog.md#make-client-interactions-dependable-and-evolvable), [protocol compatibility design](../design/protocol-compatibility.md), [clustered outcome design](../design/clustered-outcome-contract.md), [ADR 0004](0004-multi-raft-first-distributed-engine.md), [ADR 0026](0026-semantic-engine-error-classification.md), [ADR 0029](0029-local-typed-dead-letter-move-identities.md)

## Context

At the baseline, the public listener reads one JSON object per TCP line. The
protocol, client, and server declare the same v1 support range in Rust source,
but no bytes negotiate it at runtime. The client sends sequential requests;
the server bounds request bodies but has no response-frame bound or negotiated
directional limits. V1 carries arbitrary payload bytes as padded base64 and
returns errors with a code and diagnostic message, without an authoritative
wire outcome or processing stage. The reusable client conservatively
classifies transport ambiguity locally and never automatically retries.

The accepted [engine classification in ADR 0026](0026-semantic-engine-error-classification.md)
provides a backend-independent failure kind and safe attempt outcome, but no
processing stage. The [clustered outcome design](../design/clustered-outcome-contract.md)
shows why a timeout after a mutation may have crossed a durability point and
why topology-specific stages must not become client obligations. The public
protocol therefore needs an explicit version/capability handshake and a
conservative result envelope before a later runtime can claim interoperable
retry semantics.

Primary protocol references include [Kafka's protocol guide](https://kafka.apache.org/43/design/protocol/),
[NATS's client protocol](https://docs.nats.io/reference/protocols/client),
[PostgreSQL's protocol overview](https://www.postgresql.org/docs/current/protocol-overview.html),
[RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html), and the
[Protocol Buffers v3 guide](https://protobuf.dev/programming-guides/proto3/)
and [encoding guide](https://protobuf.dev/programming-guides/encoding/).
Kafka's common-version discovery and reconnect behavior, NATS's advertised
limits, PostgreSQL's startup negotiation, TCP's stream framing, and Protobuf's
typed byte fields/evolution rules inform the selected design. Their API,
security, deployment, and compatibility guarantees do not transfer to Runnel.

## Decision

### Protocol and negotiation

The existing JSON-lines implementation is provisional v1 and has no
cross-release compatibility promise. The first stable direction is negotiated
v2.0, using Protocol Buffers v3 for typed request and response envelopes. The
initial client and server advertise exactly major 2, minor 0; a later minor is
advertised only after its compatible behavior is implemented. Its
logical payload is an opaque byte field for both text and binary client helpers;
stream and consumer identifiers and optional ordering keys remain UTF-8 text.
The initial v2 release has no compression.

A v2 connection starts with the exact eight-byte preface `52 4e 4c 4e 01 00 00 00`:
ASCII `RNLN`, bootstrap revision 1, and three zero reserved bytes. This
revision identifies the negotiation framing only, not the application major.
When the application listener is secured, TLS completes before this preface;
TLS 0-RTT is disabled for application protocol connections, and the client
sends no preface, credential, or application bytes before the full handshake
completes. The default loopback listener with no security configuration and
the explicitly named insecure development/test override remain plaintext.
ADR 0035 defines those listener conditions and the authorization boundary.
Any client configured with a bearer credential must establish and validate TLS
before sending the preface; it never offers the credential over plaintext,
even if a plaintext peer claims authentication is required.
The client then sends a four-byte unsigned big-endian body length and a Protobuf
Hello body no larger than 16 KiB. Hello and its reply are bounded before
allocation. Invalid preface, malformed or truncated Hello, and oversized Hello
framing close the connection without a refusal. A parseable Hello with no
common major/minor, an unsupported required capability, an invalid set or
range, or an unusable limit receives a typed refusal and then closes. Refusals
mean no application operation was attempted.

Hello advertises inclusive minor-version ranges by major, offered and required
capabilities, and the client's outbound and inbound frame-body ceilings. Each
positive major appears once and each inclusive minor range has a minimum no
greater than its maximum. The server selects the highest common major and then
the highest common minor for it, returns only offered capabilities it
supports, its maximum inbound and outbound body sizes, and the effective
limits. Client-to-server selection is
the minimum of the client's outbound ceiling and the server's inbound ceiling;
server-to-client selection is the minimum of the client's inbound ceiling and
the server's outbound ceiling. The client validates those exact minima against
the Hello fields, along with the selected version, capabilities, and explicit
authentication requirement. Every successful Hello reply includes the
optional Protobuf boolean `auth_required`; its presence is mandatory, and
omission is a protocol violation that makes the client close without sending
credentials or an application request. The server may set it to false only
when security configuration is absent and the listener is loopback, or when
the explicit insecure development/test override is enabled. TLS or credential-policy
configuration requires it to be true. Unknown optional capabilities are
ignored. Capability names are case-sensitive ASCII
identifiers matching `[a-z][a-z0-9_]{0,63}`. The offered and required lists
are sets with no duplicates, and every required capability must also be
offered; duplicate names, malformed names, an invalid version range, or a
required capability not present in the offer receives `invalid_hello` and
closes the connection. A client must require every capability
needed for its request and must not send an operation that depends on an
unselected capability. Core behavior belongs to the negotiated version.
The bounded bearer-authentication exchange is core v2 control flow, not an
optional capability; every v2 implementation must understand it. Later
optional operations may be capability-gated without this ADR fixing their
operation names or fields. In particular, a future `consume_batch` capability
can gate the consume-batch operation; clients must require it in Hello before
sending any operation defined by that capability. If the server does not
support a required capability, it refuses and closes before application
traffic. V2 begins at exactly 2.0, uses one connection-scoped version, and
does not negotiate independent operation versions. A reconnect repeats
negotiation and authentication.
A limit below 1 KiB returns `limit_too_small`; a ceiling above its directional
hard maximum returns `limit_too_large`. The client sends no post-Hello frame
until it has received and validated a successful Hello reply. A refusal means
no application operation was attempted. If `auth_required` is true, the
client sends exactly one bounded `bearer_auth` control
frame containing its bearer credential and waits for the server's
`authenticated` control reply before sending any application request. The
authentication request and reply each have a maximum body of 1 KiB, also
subject to their negotiated directional frame limits. A client without a
credential closes without sending an application request. An incomplete
authentication exchange remains subject to the configured connection and
request deadlines. A well-framed `bearer_auth` with an empty or invalid
credential, or an application operation sent before successful authentication,
receives only a generic `authentication_failed` control response and the server
closes the connection; malformed or over-limit framing closes without parsing
or dispatch. Authentication failures do not reveal whether a credential is
unknown, invalid, or absent. If the auth deadline expires before completion,
the connection closes without an application response. Authentication control
has no application outcome or stage because no operation is dispatched. If
`auth_required` is false, the client sends no credential and may begin
application traffic after validating Hello. A client configured with a
credential treats false as a configuration/security mismatch and closes
without sending that credential or an application request. If the server
receives a `bearer_auth` control frame when false, it closes without examining
the credential or accepting an application request. When `auth_required` is
true, no ordinary operation, including Health, is accepted until authentication
succeeds; when false, Hello completion is sufficient to begin application
traffic.

After Hello, every frame is a four-byte unsigned big-endian body length and
one Protobuf v3 envelope. Length counts encoded body bytes and excludes the
prefix. Post-Hello control and application traffic is sequential: one control
request and reply during authentication, then one application request and its
response at a time. There is no pipelining, multiplexing, out-of-order
completion, or correlation ID. Zero, truncated, malformed, or over-limit
frames close the connection. Readers enforce body bounds before allocation
and never scan for a later frame boundary. The client envelope distinguishes
the core `bearer_auth` control variant from top-level operation variants; the
server envelope distinguishes `authenticated` and generic
`authentication_failed` control replies from operation replies.
Hard body limits are 64 MiB client-to-server and 65 MiB server-to-client. The
server defaults to a 1 MiB request body and may configure a lower value or up
to the hard maximum. It defaults to a 65 MiB response body and may configure a
lower value only. The Hello reply includes the server's maximum inbound and
outbound body sizes and the effective limits. Client-to-server selection is
the minimum of the client's outbound ceiling and the server's inbound ceiling;
server-to-client selection is the minimum of the client's inbound ceiling and
the server's outbound ceiling. Each endpoint limit must be at least 1 KiB and
no higher than its hard maximum. A lower value is refused with
`limit_too_small`; a higher value is refused with `limit_too_large`. The client
validates the server's advertised limits against the same bounds and verifies
the effective minima exactly. Limits include the complete Protobuf
envelope, including metadata and byte fields, and exclude the four-byte length
prefix. The 16 KiB Hello cap is separate. The existing publish-batch ceiling
of 1,024 records remains; all encoded bytes also fit the negotiated request
bound. A publish request ID is limited to 1 KiB of UTF-8 bytes.
Every operation must ensure its complete response fits the negotiated response
limit before causing an application effect. If it cannot fit, the broker
returns a bounded `response_too_large` rejection with proof that no effect was
applied. It must not mutate consumer delivery/checkpoint state, append a
publish, or truncate a reply. Connection count, in-flight work, deadlines, and
storage admission are local controls, not negotiated capabilities or resource
reservations.

Within a major, existing field numbers, wire types, presence, defaults, and
meanings do not change. New minor-version fields are optional with safe
omission behavior; a sender relies on them only when the selected minor or a
capability defines them. Removed field and enum numbers are reserved and never
reused. Peers do not act on fields outside the selected schema or capability.
Protobuf binary APIs can preserve unknown fields, but conversion or
reconstruction may discard them, so a sender cannot rely on an older peer
applying or round-tripping a field it does not know. Unknown operation
variants are rejected while the connection remains open only if framing is
still synchronized. An unknown required outcome or stage
cannot be interpreted safely and makes the client classify the attempt as
unknown and close. Stable ASCII error codes match `[a-z][a-z0-9_]{0,63}` and explain diagnostics
without replacing the outcome class. Optional human-readable diagnostic text
is capped at 512 UTF-8 bytes.

### Application outcomes and stages

Every scalar server application reply and every batch item result, including
success, carries an outcome and the furthest authoritative processing stage. The outcome governs safe caller
action; the stage records evidence and does not by itself prove absence of an
effect:

| Outcome | Required meaning |
| --- | --- |
| Confirmed | The operation reached its documented success point and the reply contains its result. State changes require the durable stage; reads require completed. |
| Rejected | The broker proves that the requested application effect did not occur and the request or current precondition must change before retry. |
| Retryable | The broker proves that no effect occurred and the same intent is safe to attempt later. If engine execution began, it must provide an authoritative no-proposal/no-effect classification. Caller policy determines when to retry. |
| Unknown | The effect or result cannot be established. The caller must not assume it was unapplied. |

The public stage vocabulary is `received` (complete application frame decoded),
`validated` (operation validation passed before engine execution),
`execution_started` (engine processing began and an effect may have occurred),
`durable` (the requested state-changing effect reached its documented durability point),
`completed` (read-only operation completed), and `unknown` (furthest stage
cannot be established). A `retryable` reply requires proof that no effect
occurred and that the same intent is safe to attempt later. It may report
`received` or `validated`; it may report `execution_started` only when the
engine returned an authoritative retryable classification proving no proposal
or effect. `rejected` also requires proof of no requested effect and may report
`execution_started` only when the engine returned a definitive no-effect
result. If effect is uncertain, the outcome is unknown. Stage alone does not
prove non-application.

For the current local engine, durable state changes mean the applicable stream
append or consumer-state sync. For the current clustered engine they mean
quorum commit and durable state-machine application, as accepted in ADR 0004.
A stream create is not confirmed until its activation/reconciliation point is
complete. The response never includes a `response_written` stage: a client that
decodes the complete reply knows it received it, while a write failure cannot
report its own delivery. Once a client begins writing an application frame,
a timeout, disconnect, partial write, malformed reply, or response-size failure
without an authoritative reply is unknown. Connect and local validation
failures before application-frame writing are client-side pre-send results,
not wire stages. Clients do not automatically retry.

A scalar operation reply or batch-level error carries one outcome and stage. A
completed batch response has no aggregate outcome; it contains ordered item
results, each with its own outcome and stage. Operation-specific result detail
(for example, whether an acknowledgement newly confirmed or was already
confirmed) remains separate from the four safety outcomes. If no complete
batch response is received, unresolved items are unknown. An envelope-level
response never implies batch atomicity.

### Stable publish identity

The v2 stable identity remains publish-only and per-record for batches. A
request ID must contain 1 to 1,024 valid UTF-8 bytes, is scoped to a stream,
and must be unique among clients publishing to that stream. ID equality is
exact byte equality with no Unicode normalization. The broker forwards the ID
unchanged and retains it with its original record for at least that record's
retention lifetime. The initial implementation has no message-retention policy
and the v2 contract adds no independent ID expiry window. A future retention
policy must not expire an ID while retaining its record; the replay safety
guarantee ends when both become eligible for removal.

For request-ID comparison, canonical key bytes are the exact UTF-8 key bytes
after protocol decoding, with an absent key and an empty string both represented
as zero bytes. They are equivalent for this comparison because the current
local durable representation cannot distinguish them; this does not make them
equivalent for ordering semantics, where an empty key can express ordering
intent and an absent key cannot. Non-empty keys are compared byte-for-byte,
without normalization. The fingerprint contains the stream, these canonical
key bytes, and exact logical payload bytes. The server-assigned timestamp and
request ID are excluded. Repeating an ID with the same comparison inputs
returns the original receipt without appending another record. Reusing it with
different comparison inputs maps to `request_id_conflict` only after the
serialized engine comparison proves the requested publish was not appended.
The v2 response may then report `rejected` at `execution_started`, because
that stage is paired with affirmative evidence that no publish effect occurred;
the original record and identity mapping remain unchanged, no stream offset is
allocated, and no consumer state changes. A clustered comparison command may
itself commit and apply to order the comparison, but that does not mean the
requested publish was applied. If an error does not establish the no-append,
no-effect result, the server must retain the conservative retryable or unknown
classification and must not report `request_id_conflict` as a proven rejection.
This changes provisional v1 behavior, which returns the original offset without
checking key or payload. ADR 0029 continues to describe current v1 and storage
identity behavior; this ADR supersedes that mismatch rule for negotiated v2.
The implementation must compare intent in both local and clustered engines
before v2 is claimed as supported.

No generic operation ID or correlation ID is added for poll, acknowledgement,
create, or other operations. Unknown outcomes for operations without a stable
identity require application inspection or an explicit duplicate-versus-loss
decision. A request ID does not provide exactly-once application processing
or batch atomicity.
### Compatibility, reconnect, and rollout

V2 negotiates a connection only. A reconnect always repeats the preface and
Hello and reselects version, capabilities, and limits. No selection changes
in-place. The peer and mismatch rules are:

| Peer or condition | Required behavior |
| --- | --- |
| Parseable Hello with duplicate or invalid major/minor ranges, malformed or duplicate capability names, or a required capability absent from the offer | Server sends `invalid_hello`, closes, and processes no application operation. |
| No common major/minor | Server sends `unsupported_version`, then closes. |
| A required capability is not supported | Server sends `unsupported_capability`, then closes before application traffic. |
| An unusable negotiated limit | Server sends `limit_too_small` or `limit_too_large`, then closes. |
| Malformed or unsupported preface/Hello framing | Receiver closes without guessing a legacy codec. It sends a refusal only when a valid Hello was parsed. |
| Successful Hello reply omits `auth_required` | Client treats the reply as a protocol violation, sends no credential or application request, and closes. |
| `auth_required` is true and the client has no credential | Client closes without sending an application request; the server dispatches no operation. |
| `auth_required` is true and a well-framed `bearer_auth` has an empty or invalid credential, or an application operation arrives first | Server sends only generic `authentication_failed` and closes; it does not dispatch the operation or reveal credential state. |
| `auth_required` is false but the client is configured with a credential | Client treats this as a security/configuration mismatch, sends neither credential nor application request, and closes. |
| TLS handshake is incomplete or fails | No RNLN preface, credential, or application data is sent or accepted; the transport closes. |
| Client has a bearer credential but cannot establish and validate TLS | Client closes before sending the RNLN preface and never sends the credential over plaintext. |
| TLS 0-RTT data contains a credential, preface, or application request | Client does not send it and server does not accept it for protocol processing. |
| Server selects a version, capability, or limit outside the client offer | Client treats the reply as a protocol violation, sends no application request, and closes. |
| v1 client connects to v2-only listener | Listener rejects the non-v2 preface and closes; it does not parse JSON-lines or promise a v1-readable refusal. |
| v2 client connects to v1-only listener | Client fails the bounded handshake on EOF, timeout, or non-v2 response; it sends no application request and never retries as v1. |
| Reconnect after peer restart, leader change, or failure | Client repeats transport setup, the complete preface and Hello, and required authentication; no prior selection, limits, or authentication carry over. |

The initial v2 release replaces provisional v1 at a coordinated breaking
release boundary for server, reusable Rust client, and CLI. Its listener is
v2-only. It does not dual-dispatch v1, add a separate transition listener,
expose implicit v1 mode, or silently fall back. If evidence later establishes
an independently deployed v1 population, a separate decision must define a
bounded transition and removal point.

This decision does not promise mixed-version cluster upgrades or rollback,
and public negotiation does not establish compatibility of internal peer RPC
or on-disk state. Those boundaries require separate decisions and evidence.
ADR 0035 owns the TLS profile, listener configuration and defaults, credential
format, authentication policy, and roles. This ADR defines their public
protocol ordering and bounded control exchange only.

### Rationale and alternatives

Kafka's version discovery and reconnect behavior support a mutual selected
range, while Kafka's per-API version matrix would add unnecessary dimensions
for Runnel's first negotiated release. NATS demonstrates useful advertised
limits but uses a server-first INFO message rather than mutual range selection.
PostgreSQL demonstrates separating versioned startup from application traffic.
TCP is an ordered byte stream and needs explicit application framing. Protobuf
provides direct byte fields and a documented field-number evolution model that
fits future language clients, at the cost of generated-schema tooling and
stricter unknown-field rules. These comparisons inform Runnel's decision;
they do not establish compatibility with those products.

Length-delimited JSON would reuse current Serde tooling but retain base64
expansion for opaque bytes and lack a typed field-number policy. CBOR supports
bytes with less schema tooling but would leave Runnel to define more of its
schema evolution rules. The project accepts Protobuf's schema and codegen cost
for a typed, language-neutral public boundary. Compression and per-operation
versioning are deferred until measured workloads or independent API evolution
justify them.

## Consequences

- The current listener and client remain unchanged until a separate runtime
  implementation adopts this contract. Current v1 tests and support constants
  are not compatibility evidence.
- A v2 runtime requires generated Protobuf schemas, bounded frame readers and
  writers, Hello negotiation, typed refusals, and changed v1 client/server
  behavior at a coordinated breaking boundary.
- The client can trust explicit outcomes instead of maintaining code-based
  retry lists. An outcome still does not automate retry policy or provide a
  generic resolution identity.
- Reusing a publish ID with changed content becomes a definitive conflict in
  v2. Both engines need to compare the retained intent; this is an intentional
  difference from current v1 behavior and requires restart and leader-change
  evidence before release.
- V2 limits bound encoded frame bodies in each direction. They do not reserve
  process capacity or promise that a particular request will be admitted.
- No cross-language support claim, v1 transition window, peer-protocol
  compatibility, disk-format compatibility, or rollback guarantee is
  accepted here. TLS policy and authorization semantics remain governed by
  ADR 0035; this ADR accepts no security guarantee beyond its stated v2
  authentication exchange and ordering.
- TD-003 and TD-025 remain open until the implementation and real-process
  evidence gates in the [protocol compatibility design](../design/protocol-compatibility.md)
  pass. The client-interactions backlog outcome remains open.

## Verification required before runtime support

The [protocol compatibility design](../design/protocol-compatibility.md)
records the required tests. In particular, real-server tests must cover exact
preface/Hello negotiation, no-overlap and capability refusal, reconnect,
TLS-before-preface and disabled 0-RTT use, explicit auth-required negotiation,
bounded bearer authentication, generic authentication failure and connection
closure, and proof that no operation is accepted before authentication.
Additional tests cover malformed and oversized frames, both directional
limits, no mutation when a response cannot fit, every outcome/stage class in
local and clustered paths, response loss after durable application, and v2
request-ID conflict and replay across restart and leader change.
Language-neutral golden frames and an independent generated client/decoder
are required before claiming cross-language interoperability. No runtime
behavior or such test is included in this ADR.

## References

- [Protocol compatibility and evolution design](../design/protocol-compatibility.md)
- [Clustered durability and outcome contract](../design/clustered-outcome-contract.md)
- [Message encoding and compression research](../research/message-encoding-and-compression.md)
- [Apache Kafka protocol guide](https://kafka.apache.org/43/design/protocol/)
- [NATS client protocol](https://docs.nats.io/reference/protocols/client)
- [PostgreSQL protocol overview](https://www.postgresql.org/docs/current/protocol-overview.html)
- [RFC 9293: Transmission Control Protocol](https://www.rfc-editor.org/rfc/rfc9293.html)
- [Protocol Buffers v3 guide](https://protobuf.dev/programming-guides/proto3/)
- [Protocol Buffers encoding guide](https://protobuf.dev/programming-guides/encoding/)
