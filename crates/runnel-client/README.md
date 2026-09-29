# Runnel Rust client

`runnel-client` is the supported async Rust client for the current provisional
JSON-lines protocol. It keeps one TCP connection open and sends sequential
requests over it. Use the typed methods for streams, binary-safe publishing,
polling, and acknowledgements; they expose outcome classifications instead of
retrying operations silently.

## Run the application example

Start a local broker in one terminal:

```sh
just run
```

Then, from the repository root, run the application-shaped publish and consume
flow in another terminal:

```sh
RUNNEL_ADDR=127.0.0.1:4222 ORDER_ID=order-42 \
  cargo run -p runnel-client --example application
```

The example creates the `orders` stream if needed, publishes a payload that
includes non-UTF-8 bytes, polls it as `order-worker`, processes the returned
bytes, and acknowledges the message. One `Client` instance serves these normal
requests over a persistent connection. Running it again with the same
`ORDER_ID` uses the same request identity; after the consumer has acknowledged
the message, the poll returns empty. Use a new order ID for a distinct event.

The broker address defaults to `127.0.0.1:4222`. `ORDER_ID` must be non-empty
and should come from a stable application event identity. The example uses
`order-created:<ORDER_ID>` as `request_id`, retains the exact payload and
options, and, only when the publish outcome is `Unknown`, reconnects and
explicitly retries once with the same request ID and bytes. A second uncertain
result is returned to the caller for application-level resolution. Confirmed
rejections are surfaced, and the example does not spin on retryable failures.

Treat a request ID as unique to one logical publish on its stream. Persist it
with the application event so a process restart does not create a new identity
for the same operation. The current broker deduplicates repeated IDs and
returns the original offset; a new identity can create a duplicate if an
earlier publish was accepted but its response was lost. The client does not
retry automatically. For requests that time out or are cancelled after they
may have started writing, treat the outcome as unknown, replace the connection,
and decide explicitly whether to retry. Keep retries bounded and apply
application backoff where a response classifies an operation as retryable.

The crate and broker currently declare `runnel-json-lines` protocol version 1,
including UTF-8 text and base64-encoded opaque bytes. This declaration is
checked in the source; the listener does not negotiate a version at runtime, so
it is not a cross-version compatibility guarantee.

## End-to-end coverage

The example demonstrates API composition; its normal local run does not inject
a lost response or restart the broker. The real-server tests cover those
failure paths separately:

- [`client_path.rs`](../runnel-server/tests/client_path.rs) exercises one
  persistent client with binary payloads, a publish whose first response is
  dropped and whose retry reuses the stable identity, and an application flow
  that redelivers an unacknowledged binary message after broker restart before
  acknowledging it.
- [`server_smoke.rs`](../runnel-server/tests/server_smoke.rs) verifies binary
  publish and request-ID deduplication across a broker restart.

Run the focused real-server coverage with:

```sh
cargo test --locked -p runnel-server --test client_path
cargo test --locked -p runnel-server --test server_smoke network_protocol_recovers_binary_publish_batch_and_request_ids
```
