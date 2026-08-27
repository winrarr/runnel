# ADR 0008: Use a resource-limited container benchmark harness

- Status: accepted
- Date: 2026-08-20

## Decision

Use a small Python standard-library runner, invoked through `just`, to start the Runnel image with explicit Docker CPU and memory limits and execute repeatable end-to-end workloads through the current public development protocol. Emit versioned JSON result artifacts containing workload, durability, image, source, host, resource, latency, throughput, and recovery information.

Keep the benchmark runner separate from the broker implementation and keep generated results out of version control. Add Kafka, Redpanda, and NATS JetStream only through adapters that document equivalent acknowledgement, replication, ordering, and delivery semantics.

Run a small container benchmark smoke check in CI to verify the harness and image lifecycle, but do not make noisy performance measurements a pull-request gate until variance and trend reporting are understood.

## Rationale

Container limits make local measurements more repeatable and expose the broker's startup, network, storage, and process boundaries. Python's standard library is sufficient for the current line protocol and Docker orchestration, avoiding a second benchmark dependency graph while the public protocol is still provisional. JSON artifacts allow later comparison and trend tooling without coupling the broker to a reporting system.

Native competitor clients and broker-specific tools do not share one acknowledgement or durability contract by default. An adapter boundary keeps the comparison honest and allows each broker's measured guarantees to be stated explicitly.

## Consequences

- Docker and Python are prerequisites for the container benchmark workflows.
- The current runner measures Runnel's development protocol and local engine, not a future binary protocol or clustered engine.
- Resource samples are observational and must be interpreted with the host, storage, image, and workload recorded in each result.
- At the time of this decision, full cross-broker comparison and historical reporting remained planned. Those workflows now exist as separate engineering evidence, while semantically equivalent cross-broker workloads and complete recovery coverage remain future work; neither is an implicit compatibility promise.
