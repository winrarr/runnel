# ADR 0022: Add an additive binary payload representation

- Status: accepted; provisional protocol boundary
- Date: 2026-08-28

## Context

The JSON-lines protocol historically carries publish and message payloads as
UTF-8 strings. The engine already accepts opaque bytes, but converting a
delivered payload with lossy UTF-8 handling prevents arbitrary bytes from
round-tripping. The [message-encoding research](../research/message-encoding-and-compression.md)
and [day-one plan](../design/encoding-compression-day1-plan.md) recommend
keeping the inspectable JSON path while making the logical payload boundary
binary-safe. They explicitly defer a new binary wire schema, negotiation, and
compression.

## Decision

Keep the existing `publish` request and `message` response unchanged for text
payloads. Add an additive `publish_bytes` request with a `payload_base64` field
and a `message_bytes` response with the same field. The field is standard,
padded base64 and is decoded into a validated `BinaryPayload` before a request
can reach the engine. Unknown fields on requests are rejected, so a request
cannot provide both the legacy text and binary representations.

The reusable client exposes `publish_bytes` and `publish_bytes_with_options`,
plus `poll_bytes` and `poll_group_bytes`. Its existing text methods and
`Message` type remain available. A binary-aware poll accepts either a legacy
UTF-8 `message` response or a `message_bytes` response and returns exact
payload bytes. The server uses the legacy response for UTF-8 payloads and the
binary response when the bytes are not valid UTF-8. The CLI keeps its existing
text invocation and accepts `publish <stream> --payload-base64 <value>` for
binary publishing.

## Compatibility and recovery boundary

This is an additive change to the current provisional JSON-lines protocol, not
a version-negotiated compatibility contract. Existing text requests and
responses retain their field names and JSON shapes. Clients that do not know
`publish_bytes` cannot publish arbitrary bytes, and clients that only use the
legacy text poll method must use the binary-aware client method when consuming a
stream that may contain non-UTF-8 messages. Malformed base64 and contradictory
request fields receive `invalid_request` before engine execution.

Binary payloads use the existing engine and durable record path. This decision
does not change storage layout, offsets, acknowledgement, redelivery,
at-least-once behavior, request identity, limits, or restart semantics. The
binary representation is bounded by the existing request-frame and response
write limits. TLS, authentication, batching, compression, binary peer frames,
and final version negotiation remain out of scope.

## Alternatives considered

- Replacing `payload: String` with a base64 string would silently change the
  meaning of existing text clients.
- Making `payload` a new tagged union would preserve wire flexibility but
  would break current Rust callers and unowned integration fixtures.
- Adding optional text and base64 fields to the existing variants would make
  contradictory combinations part of every caller's validation burden.

Separate additive variants preserve the current text path while making the
new representation explicit and fail-closed. A future versioned wire schema
may supersede these variants after compatibility, negotiation, and performance
evidence are established.
