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

## Repository workflow constraints

- Changes must be delivered through separate pull requests, with each independently reviewable change on its own non-`main` branch.
- Direct pushes to `main` and bypassing repository rulesets or required checks are not allowed.
- The `main` ruleset requires pull-request checks but does not require every branch to contain the newest `main` commit. Contributors must check the recorded baseline and newest `origin/main` CI status before starting and before opening or updating a pull request, and refresh a branch when changes overlap in paths, contracts, dependencies, generated files, or integration behavior.
