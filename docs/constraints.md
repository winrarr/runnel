# Product constraints

These constraints come from the product brief and should guide implementation choices:

- Rust is the implementation language unless a compelling technical reason changes that choice.
- The first useful deployment is a single self-contained broker process with explicit persistent storage.
- The public model should remain small: streams, records, producers, consumers, consumer groups, acknowledgements, ordering keys, retention, and replay.
- Physical partitions, broker topology, and consensus details should not leak into normal application code.
- Durable acknowledged messages must not disappear under the selected durability guarantee.
- Delivery is normally at least once; safe retries and ambiguous outcomes must remain representable.
- Memory and buffering must be bounded, and backpressure must be explicit rather than silently dropping data.
- Future clustering, replication, failover, fencing, rolling upgrades, and larger streams must not require an application programming-model rewrite.
- The broker must remain correct without Kubernetes; container and Kubernetes support are operational surfaces, not correctness dependencies.

