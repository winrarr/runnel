# ADR 0009: Use pinned native tools for the first broker comparison baseline

- Status: accepted
- Date: 2026-08-20

## Context

Runnel's performance goals need an external baseline before storage and transport work are optimized. Kafka, Redpanda, and NATS JetStream expose different client models and their public protocols are not interchangeable with Runnel's provisional JSON-lines protocol. Waiting for a common client would delay useful measurements, while silently treating native tools as equivalent would create misleading claims.

## Decision

Maintain a separate comparison runner that starts one isolated container per broker, applies explicit broker and client CPU and memory limits, uses pinned images and native benchmark clients, and writes machine-readable JSON results. The initial workload uses one local broker, one stream/topic, one partition where applicable, replication factor one, disabled compression where the client exposes that setting, 100-byte and 1 KiB payloads, durable publish, and consumption.

The runner records the measurement boundary for every backend. Runnel and JetStream include durable publish acknowledgement paths; Kafka and Redpanda use Kafka's native producer performance client with `acks=all` and use the native consumer performance client for fetch throughput without application-level acknowledgements. These results are an engineering baseline and must not be presented as a final apples-to-apples ranking.

Use a 2 GiB default memory limit for the shared run because Redpanda's development container reserves approximately 1 GiB before process overhead. The limit remains configurable for experiments that intentionally use a different resource profile.

## Consequences

- A comparison can be rerun locally with one documented command and its exact image identifiers and limits are preserved in the result.
- The baseline can expose large resource and throughput differences before a common client exists.
- Kafka-family latency and consumer numbers must be read with their native client semantics; they are not directly comparable to Runnel's per-request latency or JetStream's explicit acknowledgement path.
- The comparison harness remains separate from correctness CI and does not gate pull requests. A common semantic workload, recovery scenarios, trend reporting, and any performance gate require later work.
