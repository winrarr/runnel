# Initial product fit

This document states Runnel's initial product thesis. It describes the audience and workloads the project is designed around; it is not a claim that every capability is production-ready today. Current behavior is defined by code and tests, while unfinished outcomes are tracked in the [product backlog](backlog.md).

## The initial audience

Runnel is for small engineering teams that need durable messaging inside one product or service estate but do not want a distributed event platform to become a system of its own.

The primary adopter is an application or platform engineer who:

- needs reliable background work, durable domain-event delivery, or both;
- wants one self-contained broker process to be a useful starting deployment;
- values low and predictable latency, bounded resource use, and understandable failure behavior over a very broad feature ecosystem;
- expects to operate the broker without a dedicated messaging team;
- may grow from one node to a highly available, scalable deployment, but wants partitions, replica placement, leadership, consensus, and rebalancing to impose as little application and operational complexity as the guarantees allow.

Runnel should feel like adopting a focused infrastructure component, not like adopting an event platform that requires its own operating model.

## The initial workloads

The first product should make these workflows dependable end to end:

### Durable background work

Several worker instances share a durable consumer. Work is delivered at least once, failed workers cause redelivery, stale acknowledgements are fenced, poison messages can be isolated, and work with the same ordering key is not processed concurrently. Unrelated keys continue to make progress.

Examples include webhook delivery, media or document processing, indexing, notification delivery, and application-owned asynchronous jobs.

### Durable application events

Independent consumers each process the same retained stream at their own durable position. A consumer can stop, restart, catch up, and deliberately replay retained history without changing how producers publish.

Examples include projections, audit-oriented event feeds, cache or search updates, and integration events between a small number of application services.

### A simple growth path

An application starts with one broker and explicit local storage. As its availability, throughput, or retained-data needs grow, it can move first to a small highly available cluster and later to larger supported deployments while continuing to use streams, records, consumers, acknowledgements, retry identity, and ordering keys. Runnel should implement the placement, replication, leadership, recovery, and balancing mechanisms required to make that scale safe, while minimizing how much of that machinery users must configure or understand. Migration and operating procedures must be supported; source-code compatibility alone is not enough.

## The product promise

The initial product is successful when it offers all of the following together:

- a small, topology-free application model;
- durable acknowledged data under an explicitly selected guarantee;
- at-least-once delivery with safe acknowledgements, visible retries, and explicit ambiguous outcomes;
- ordering only where requested, so unrelated work can proceed concurrently;
- bounded memory, storage, connections, and in-flight work with explicit backpressure or rejection;
- a supported client path with safe defaults rather than requiring applications to implement the wire protocol;
- useful readiness, metrics, diagnostics, backup, recovery, and upgrade behavior;
- a single-node deployment that is genuinely useful before clustering is required;
- a documented path from that deployment to a three-node cluster, followed by a credible path to larger supported deployments, without changing application intent.

Low latency and throughput matter within this promise. They are not substitutes for durability, bounded resource use, or ease of operation.

## What Runnel is not initially

Runnel is not initially intended to be:

- a Kafka-compatible platform or a replacement for Kafka's connector and analytics ecosystem;
- a multi-region event backbone or an active-active geo-distributed log;
- a hosted multi-tenant service with untrusted tenants and workload isolation;
- an exactly-once application-processing system;
- a transactional database or a general workflow engine;
- a system that claims effectively unlimited retention, tiered history, compaction, or very large-cluster support before those capabilities have explicit designs and evidence.

Teams that require those properties should use an established system that provides them rather than rely on Runnel's future direction.

## How product decisions should be made

Near-term work should improve the complete experience of the initial workloads before expanding the platform surface. In particular, the stable client contract, retention and disk-pressure behavior, overload control, replay, batching, security, observability, and single-node-to-cluster migration take priority over more sophisticated placement or additional distributed engines. This is sequencing, not a limit on the intended scale of the finished broker.

The three-node backend remains valuable before the single-node product is complete because it tests whether the public model and durability boundaries can grow without an application rewrite. Its role is to retire that architectural risk and establish the first distributed operating point. Later placement, balancing, membership, storage, and replication work should extend that foundation when evidence justifies it, while keeping unavoidable complexity primarily inside the broker and its administrative surfaces.

Performance work should use representative end-to-end application workflows and state the durability mode, message size, concurrency, ordering-key distribution, retained-data size, resource budget, and failure state. Competitor measurements are engineering evidence, not the definition of product fit.

## Evidence still required

This thesis is specific enough to guide development, but it still needs validation. Before calling the initial product ready, the project should establish:

- two or three representative end-to-end reference workloads with explicit latency, throughput, memory, storage-growth, recovery, and operator-effort budgets;
- onboarding and failure-recovery exercises with engineers from the intended audience;
- clear tested limits for message size, connection count, in-flight work, retained history, consumer lag, and supported deployment scale;
- evidence that the supported client, operational workflow, and one-node-to-three-node migration are understandable without broker-internals knowledge;
- documented reasons to choose or reject Runnel for each reference workload.

Until that evidence exists, this document is the product hypothesis that guides implementation and evaluation, not a market-validation claim.
