# Consumer-lag telemetry design

Status: proposal for TD-006; design-only, not an implementation or a runtime
guarantee.

This proposal adds consumer-lag visibility without making the health path scan
retained history, without turning stream or consumer names into an unbounded
Prometheus dimension, and without treating a replicated copy as another
logical consumer. The first implementation should expose a fixed-cardinality
aggregate in `/metrics` and an exact, bounded, identity-selected diagnostic
operation. It should keep `HealthSnapshot` and the existing health response
unchanged.

## Current baseline

The current implementation establishes useful boundaries but does not expose
lag:

- [`runnel_engine::HealthSnapshot`](../../crates/runnel-engine/src/lib.rs) contains only broker-wide `streams`,
  `storage_bytes`, `in_flight_deliveries`, `redeliveries`, and `dead_letters`.
  The public protocol health response projects only stream count and storage
  bytes. Adding lag fields directly to either type would couple a future
  per-consumer view to the existing health contract.
- The server's [`/metrics`](../../crates/runnel-server/src/main.rs) output uses fixed operation labels only. It already
  reports aggregate request, traffic, publish, delivery, acknowledgement,
  admission, storage, health, and clustered snapshot signals. Real-server
  tests explicitly ensure that failed protocol requests do not create stream
  or consumer labels.
- Local [`runnel-core`](../../crates/runnel-core/src/lib.rs) keeps the complete append-only stream history on disk,
  a bounded tail index and bounded sparse index, and a reconstructed
  `next_offset`. Consumer
  checkpoints are durable per `(stream, consumer)` and contain a contiguous
  `committed_offset`, out-of-order acknowledged offsets, and delivery attempts.
  Active deliveries are indexed per consumer, with expiry removed through a
  deadline index. Consumer state files are created lazily; there is no
  complete in-memory consumer catalogue.
- Clustered [`runnel-raft`](../../crates/runnel-raft/src/lib.rs) state stores stream records, consumer progress, out-of-order
  acknowledgements, attempts, leases, and grouped in-flight ownership in each
  stream data group. `GroupManager::health` sums the currently materialized
  groups on the local node after checking metadata leadership. It is not a
  cross-node metrics aggregator, and replica copies must not be summed as
  separate logical backlog.
- Local health obtains physical log-file lengths. Clustered state-machine
  health derives logical key-plus-payload bytes by iterating materialized
  messages. Neither definition is a consumer-lag byte definition, and the
  existing `runnel_storage_bytes` meaning must not silently change.
- Current retention is unlimited and the replay boundary is offset-based.
  The [retention design](retention-disk-pressure-plan.md) proposes a future logical retained floor and
  `protect`/`expire` behavior, but those are not current guarantees.

[`TD-006`](../tech-debt.md#td-006-operational-telemetry-remains-incomplete) remains open because the current signals cannot explain consumer lag,
retained or reclaimable storage, queue saturation, replication progress, or
resource pressure. This document proposes one bounded slice of that outcome;
it does not retire the debt item.

## Semantics

Lag must identify which notion of progress it measures. The proposed snapshot
uses the following values for one logical `(stream, consumer)`:

| Symbol or field | Meaning | Unit and source |
| --- | --- | --- |
| `H` | Durable stream head, the first offset after the committed stream records visible to consumers | Offset; local log's reconstructed `next_offset`, or the committed data-group head in a cluster |
| `F` | Retained floor, the first offset still available for ordinary delivery or replay | Offset; `0` while unlimited current retention is in effect |
| `C` | Contiguous durable consumer progress, equivalent to the checkpoint's `committed_offset` | Offset; durable consumer state |
| `A` | Durable out-of-order acknowledgements at offsets at or after `C` | Count/set of offsets; consumer state |
| `I` | Current deliveries returned but not durably acknowledged | Count; current in-flight index/state |
| `cursor_lag_records` | `H - C`, when `H >= C` and `C >= F` | Records; logical cursor distance, not a count of immediately deliverable records |
| `unacknowledged_records` | `H - C - |A|`, when the retained range and state are complete | Records; includes in-flight records because they are not durably acknowledged |
| `in_flight_records` | `|I|` | Records; current lease/assignment state only |
| `oldest_unacknowledged_age_seconds` | `0` when `C = H`; otherwise current wall-clock time minus the published timestamp of offset `C`, when `C < H` and that record is retained | Seconds; a sampled age, not durable elapsed time |
| `cursor_lag_bytes` | Logical key-plus-payload bytes for offsets `[C, H)` | Bytes; requires cumulative byte metadata and is not the physical file size |

The initial operator-facing name should be `cursor_lag_records` (exported as
`runnel_consumer_lag_records` for a defined aggregate). Calling `H - C` an
"unacknowledged count" would be wrong for grouped consumers because
out-of-order acknowledgements are allowed. The separate `unacknowledged_records`
field can be added only when the implementation proves that its retained-range
accounting is complete.

The following rules are part of the proposal:

1. `H` is a durable/committed head, never an assigned but uncommitted offset.
   A cluster follower may not report its local physical tail as fresh lag.
2. In-flight work is not subtracted from cursor or unacknowledged lag. It is
   work the application has not durably completed and can be redelivered after
   expiry or restart. `in_flight_records` is reported separately so an operator
   can distinguish a worker currently processing messages from records that
   have not yet been assigned.
3. `cursor_lag_records` is an offset-distance upper bound on records behind the
   durable cursor. It is not `ready_records`: current keyed delivery can skip
   acknowledged, leased, or same-key-blocked records, and counting candidates
   may require a scan. The first slice should not publish a made-up ready
   count.
4. A non-grouped consumer has an independent durable cursor. A shared consumer
   group has one durable cursor and transient member names. Lag is therefore
   per `(stream, consumer)` group, never per member. Member names, ordering
   keys, delivery tokens, and offsets are diagnostic detail, not metric labels.
5. `replay` is read-only in the current protocol and does not change `C`, `A`,
   `I`, or lag. A future durable replay session must contribute its own
   retention pin and status rather than being folded into consumer lag.
6. Dead-letter streams are ordinary streams for telemetry. Source lag reflects
   the source acknowledgement/dead-letter transition, and a dead-letter
   consumer has its own independent lag. No special label is needed.
7. A stream with no known consumers has a known aggregate of zero only when
   the consumer catalogue is complete. A zero with an incomplete catalogue is
   not evidence that all consumers are caught up.

### Retention and unavailable history

Unlimited retention currently means `F = 0`. A future `protect` policy may keep
`F` at or below the slowest consumer's durable progress. A future `expire`
policy may advance `F` beyond a consumer's `C`. In the latter case the
telemetry must report `retention_expired` (or an equivalent explicit status),
include `F`, `C`, and `H` in a diagnostic response, and omit numeric lag that
would imply the missing records were processed. It must not silently replace
`C` with `F`, return `H - F`, or turn the condition into an ordinary empty
poll.

The same rule applies to a replay session that has lost its retained range.
Physical cleanup can lag a logical floor; physical `storage_bytes` and logical
consumer lag remain separate. A retained floor does not by itself say how
many bytes are reclaimable, because segment boundaries, acknowledged history,
active deliveries, and replay pins may differ.

### Age and bytes

Record lag can be computed from `H` and `C`. Age and bytes need more care:

- The current tail and sparse indexes do not guarantee a bounded lookup for an
  old `C`; a cold lookup can scan from an old checkpoint or byte zero. A
  health/metrics scrape must not perform that scan. Until an indexed timestamp
  is available, omit `oldest_unacknowledged_age_seconds` for a cold consumer
  and mark the observation `unknown` rather than returning a slow or invented
  value.
- `cursor_lag_bytes` must use a cumulative logical byte total (key bytes plus
  payload bytes) at each indexed head/floor boundary. It must not reuse local
  frame length, sparse-index bytes, JSON state bytes, Raft log bytes, snapshot
  bytes, or the current storage gauge. New and reopened stores need the same
  accounting; a rebuild that has not completed is `unknown`.
- An exact unacknowledged byte count also needs acknowledged-offset byte
  accounting. It should remain out of the first slice unless the state stores a
  bounded prefix or equivalent per-offset size information. A byte metric that
  silently means `H - C` times an average payload is not acceptable.

## Collection and aggregation model

Lag should be a separate, optional engine telemetry capability rather than an
extension of `HealthSnapshot`. Conceptually it returns a bounded
`ConsumerLagSnapshot` with an observation timestamp, source revision, coverage
status, and either aggregate values or one explicitly selected consumer. A
default capability result is `unknown`; an engine that cannot provide lag must
not be forced to fabricate zeroes.

### Exact selected-consumer observation

The first exact interface should accept one validated stream and consumer
identity and return at most one bounded record. It should read the stream head
and consumer state from one consistent stream/data-group revision. The result
can include:

```text
stream, consumer
source_revision
observed_at_ms
head_offset_exclusive
retained_floor_offset
committed_offset
cursor_lag_records
unacknowledged_records (when complete)
in_flight_records
oldest_unacknowledged_published_at_ms (when indexed)
status = fresh | stale | unknown | retention_expired
```

The exact transport and authorization boundary are not part of this design.
If exposed through a future administrative operation, it must be authenticated
and capability/version gated; the current unauthenticated HTTP surface must
not gain a consumer-name inspection endpoint by implication. The operation is
intentionally identity-selected so its work and response size are bounded.

### Local engine

Under the stream lock, the local engine already has or can derive `H`, `C`,
out-of-order acknowledgement count, and active in-flight count without reading
payloads. A named observation should use that state and a bounded metadata
lookup for the oldest pending record. It must not enumerate every log record
on a metrics scrape.

The current local state files are authoritative but are discovered lazily and
there is no complete consumer catalogue. Exact broker-wide aggregates require
one of these explicit choices before implementation:

- add a durable per-stream consumer catalogue/summary with a validated
  configured maximum and a bounded recovery path; or
- define aggregate coverage as only a bounded configured/allow-listed set and
  report `coverage_complete = 0` when other durable consumers may exist.

This proposal selects the second choice for the first safe slice: exact named
inspection plus fixed-cardinality aggregates only when the engine has a
complete, bounded summary. It does not authorize a `read_dir` walk or a full
state-file scan during `/metrics`. Introducing a broker-wide maximum consumer
count remains an unresolved product decision because current consumer names
are implicitly created and have no delete operation.

If a summary is later added, maintain it from durable state transitions and
rebuild it once at startup from bounded metadata. A partial or failed rebuild
must expose `unknown`/`truncated`, not a partial result described as the
broker-wide total. Persistence of derived lag metadata is a storage change and
requires crash/recovery and compatibility evidence.

### Clustered engine

The static cluster has one logical data group per stream and replicates
consumer progress and grouped in-flight state in that group. A future lag
query should:

1. resolve the stream through the metadata group;
2. obtain a committed, leader-authoritative view from that stream's data group
   or a clearly marked cached view with its applied revision;
3. aggregate at most once per logical stream/data-group identity; and
4. return per-group `unknown` if leadership, initialization, snapshot
   installation, or the bounded query fails.

`GroupManager::health` is currently a local-node aggregate. It is useful as a
broker health signal but must not become a cross-node lag reducer: summing the
same stream's RF=3 state from three node scrapes would triple the logical lag.
The preferred cluster metric path is a leader-authoritative logical aggregate
served from any node through a bounded forward/query. If that is not yet
available, document `/metrics` as node-local and require dashboards to select
one source; never sum replica scrapes. A follower's stale value may be used
only with `stale` status and a source revision, never as a fresh committed
cluster result.

The metadata group must not be mistaken for a consumer-bearing stream group.
The implementation should deduplicate by stable stream/data-group identity and
avoid counting metadata bookkeeping, dead-letter derivation, or physical
replica copies as additional consumer lag.

## Metrics and cardinality

The default `/metrics` surface should add only fixed-cardinality aggregate
families. Suggested names are deliberately explicit about aggregation and
units:

| Metric | Type | Definition |
| --- | --- | --- |
| `runnel_consumer_lag_records` | Gauge | Sum of `cursor_lag_records` over all covered logical consumers; omit when coverage is incomplete |
| `runnel_consumer_lag_max_records` | Gauge | Maximum `cursor_lag_records` over covered consumers; omit when coverage is incomplete |
| `runnel_consumer_lag_oldest_age_seconds` | Gauge | Maximum known oldest-unacknowledged age; omit when no complete age sample exists |
| `runnel_consumer_lag_consumer_count` | Gauge | Number of logical consumers included in the snapshot |
| `runnel_consumer_lag_coverage_complete` | Gauge | `1` when all consumers in the declared scope were observed; `0` for partial/truncated coverage |
| `runnel_consumer_lag_unknown_consumers` | Gauge | Known consumers whose source/status is unknown in this snapshot |
| `runnel_consumer_lag_expired_consumers` | Gauge | Known consumers behind the retained floor |
| `runnel_consumer_lag_snapshot_available` | Gauge | `1` only when the aggregate snapshot is fresh and complete; otherwise `0` |
| `runnel_consumer_lag_snapshot_age_seconds` | Gauge | Age of the last attempted snapshot, when an observation timestamp exists |
| `runnel_consumer_lag_snapshot_failures_total` | Counter | Bounded lag snapshot attempts that failed or timed out |

`coverage_complete` describes identity coverage, not freshness: it is `1`
only when every consumer in the declared scope was attempted. The numeric
aggregate is emitted only when that scope is complete, every required source
is fresh, and no consumer is retention-expired. Thus a complete catalogue can
still have `snapshot_available = 0` when one source is stale, unknown, or
expired.

The aggregate sum is a sum of logical per-consumer cursor distances. It is
not physical stream backlog, retained bytes, or a safe autoscaling signal by
itself. Alerting should combine it with consumer count, oldest age, publish
and acknowledgement rates, and the existing in-flight/admission signals.

No metric in the first slice should have `stream`, `consumer`, `member`,
`offset`, `key`, payload, request ID, or delivery token labels. The current
server convention already avoids caller-controlled stream and consumer
labels. Prometheus recommends minimal labels and warns that user or resource
identities can create unbounded time-series cardinality; this proposal keeps
that boundary even though per-consumer lag is useful.

If future operations need identity-labelled metrics, require a static,
validated operator allowlist with an explicit maximum series budget. Do not
implement dynamic top-K labels as the default: churn can create unbounded
historical time series even when the current scrape contains only K entries.
An allowlist would use a separate metric family from the aggregate and would
need a bounded exposition byte limit, fixed identity count, and tests for
series eviction/configuration changes. Until then, selected-consumer
diagnostics are the identity-bearing surface.

The collector must not allocate one task, timer, metric child, or payload copy
per consumer or record. It should use maintained counters/indexes and a
bounded snapshot buffer. Configuration must bound the number of selected
observations, aggregate work, serialized bytes, and concurrent lag requests.
Exceeding a bound is `truncated`/`unknown` and increments a fixed counter; it
must not block publish, poll, acknowledgement, shutdown, or ordinary health.

## Freshness, unknown state, and health interaction

The following status meanings are proposed:

- `fresh`: the selected source revision and all values required by the metric
  were read within the configured observation deadline.
- `stale`: a last-known value exists but exceeds the freshness budget or comes
  from a follower/cache behind the required committed revision. It may be
  returned by an identity-selected diagnostic operation with its age and
  revision, but must not be emitted as a fresh aggregate.
- `unknown`: a state file/index could not be read, the source group was not
  initialized/leader-authoritative, metadata accounting was incomplete, or a
  work/byte/deadline budget was exceeded.
- `retention_expired`: `C < F`; the old backlog is no longer available and
  numeric lag is intentionally omitted.

Unknown and expired are not zero. For `/metrics`, follow the existing fallback
behavior: keep the HTTP response scrapeable, emit the fixed process/admission
metrics and availability/status gauges, and omit engine-derived numeric lag
families that do not have a fresh complete value. Do not emit a fresh-looking
zero after a timeout. A stale value is similarly omitted from the default
numeric aggregate unless a separate, explicitly named last-known metric is
accepted later.

The existing one-second bounded engine health timeout applies to readiness and
metrics. Lag collection must not lengthen it or make readiness depend on
application backlog. `/health/ready` should continue to mean that the broker
can serve its declared durable workload: a large lag, an expired consumer, or
an unavailable optional lag observation does not by itself make the broker
unready. An engine health timeout still makes readiness fail and should keep
the current `runnel_health_check_failures_total` behavior. A lag query that
times out should use its own bounded failure/unknown result, not turn the
health response into a false healthy zero.

For predictable scrape latency, a future implementation should prefer an
atomically published/cached summary for aggregate metrics. A named query may
use a bounded engine operation, but it must have a deadline no greater than
the caller's request budget and must not consume the reserved health/shutdown
capacity. If a cache is used, export its age and source revision so operators
can distinguish service health from telemetry freshness.

## Compatibility and non-effects

This proposal is additive and intentionally does not change current behavior:

- Do not add fields to `HealthSnapshot`, the existing protocol `Health`
  response, or `Message`. Existing health fixtures and clients remain valid.
- Keep the current meanings of `runnel_storage_bytes`,
  `runnel_in_flight_deliveries`, redelivery/dead-letter counters, request
  metrics, and snapshot metrics. Consumer lag is not a reinterpretation of any
  existing gauge.
- Do not expose physical log paths, Raft groups, replica placement, sparse
  indexes, or offsets as new ordinary client concepts. A selected diagnostic
  operation is an authenticated operational surface with explicit versioning,
  not a replacement for the provisional messaging protocol.
- Do not change delivery, acknowledgement, expiry, fencing, ordering, replay,
  dead-letter, retention, or durability semantics. Lag observes those state
  transitions; it must not advance a consumer or pin/delete history merely to
  report it.
- Additive Prometheus families may be ignored by existing scrapers. Existing
  scrape fallback and readiness status remain compatible; unavailable lag is
  omitted/marked unknown rather than represented as zero.

Before exposing a new protocol or administrative operation, define capability
negotiation/version behavior, authorization, response size limits, and the
unknown/expired outcome. A Rust trait extension should have a default unknown
implementation or be an optional telemetry capability so third-party engine
implementations are not broken by a lag-only method.

## Reference comparison

These references inform the proposal but do not make Runnel compatible with
their partition, subscription, or monitoring models.

| Reference | Useful precedent | Difference that matters to Runnel |
| --- | --- | --- |
| [Apache Kafka consumer metrics](https://kafka.apache.org/41/generated/consumer_metrics.html) | Exposes `records-lag` per topic/partition and `records-lag-max`; the documentation distinguishes current consumer position from committed offset. | Kafka has explicit partitions and consumer-side assignment. Runnel has one logical stream with independent cursors and transient shared-consumer members, so `H - C` and group in-flight state need names that do not imply a partition or member metric. |
| [NATS JetStream `ConsumerInfo`](https://nats-io.github.io/nats.js/jetstream/types/ConsumerInfo.html) and [pull-consumer guidance](https://docs.nats.io/learn/jetstream/pull-consumers) | Separates pending messages, acknowledgement-pending messages, delivered sequence, acknowledgement floor, redelivery count, and waiting pulls. | The separation supports Runnel's distinction between cursor lag, out-of-order acknowledgements, and in-flight work. NATS's server-managed consumer info is not a reason to scan Runnel's retained file or to expose member identity as a metric label. |
| [Google Cloud Pub/Sub monitoring](https://docs.cloud.google.com/pubsub/docs/monitoring) | Uses unacknowledged count and oldest-unacknowledged age as complementary backlog signals, and documents that backlog samples can have gaps/delay. | Runnel can provide exact committed offset distance for a selected source, but must make its own one-second health deadline, retention floor, and cluster revision explicit rather than hiding them behind a managed service's sampling model. |
| [Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/) and [metric naming/cardinality guidance](https://prometheus.io/docs/practices/naming/) | Treats current state as gauges, recommends timestamps for elapsed age, and warns against high-cardinality identity labels. | Runnel's current fixed-label metrics convention and untrusted validated names favor aggregate gauges plus an identity-selected diagnostic operation; dynamic per-consumer series are not a safe default. |

## Hypotheses and unresolved risks

The following are hypotheses to measure or resolve during implementation, not
claims about the current runtime:

- **H1 — maintained summaries are cheaper and safer than scrape-time scans.**
  Persisted head/byte metadata and an explicit consumer summary should make
  aggregate collection bounded, but publish/ack update work, startup rebuild,
  and lock scope need measurement under many consumers.
- **H2 — record lag is useful without ready-count semantics.** `H - C` plus
  in-flight and oldest-age signals may explain most backlog incidents, while
  keyed candidate eligibility remains intentionally a separate future metric.
  Slow-consumer and hot-key workloads must test this assumption.
- **H3 — a leader-authoritative per-stream query is sufficient for cluster
  operations.** It avoids replica double-counting, but forwarding every
  identity query or maintaining an aggregate cache may add latency and create a
  failure dependency. The design must choose and test one bounded source.
- **H4 — logical byte lag is worth the extra metadata.** Variable payloads,
  out-of-order acknowledgements, retention floors, and frame-format changes
  may make record lag plus age the more robust first release.

Unresolved decisions before implementation are:

1. Whether to introduce a maximum number of durable consumer states or accept
   incomplete broker-wide aggregates when the current unbounded implicit
   consumer model exceeds the telemetry budget.
2. Whether the first aggregate is a complete durable summary, a configured
   allowlist, or only fixed broker-wide health/coverage signals plus exact
   selected inspection.
3. The source-revision and forwarding protocol for a leader-authoritative
   cluster query, including behavior while a stream data group is being
   materialized or a replacement snapshot is installing.
4. Whether age and logical byte lag belong in the first release or require the
   segmented storage/retention work to provide bounded timestamp and prefix
   indexes.
5. The authenticated administrative transport and capability/version contract
   for identity-selected inspection. No unauthenticated endpoint is implied.
6. How a future replay session and retention policy contribute pins and
   `retention_expired` status without pretending that physical cleanup is
   instantaneous.

## Test and acceptance gates

This design-only change is classified as **Design or research**. No runtime
behavior or performance claim is made, so implementation benchmarks are not a
gate for this document. The future implementation should satisfy the
following staged gates.

### Stage 0 — accept the contract

- Record the definitions of `H`, `F`, `C`, `A`, `I`, status, source revision,
  coverage, and units in an accepted decision record before changing the
  engine contract or storage format.
- Select the durable-consumer catalogue/summary policy, metric names, bounds,
  authorization, and compatibility/version behavior.
- Establish a fake clock and deterministic source-revision seam for unit
  tests, while retaining real filesystem/process tests for failure behavior.

### Stage 1 — exact local observation

- Unit tests cover empty streams, head equals committed offset, out-of-order
  acknowledgements, grouped members, in-flight leases, expiry/redelivery,
  restart-recovered attempts, and dead-letter transitions.
- Reopen tests prove that lag is unchanged across checkpoint journal replay and
  any persisted head/byte metadata recovery. A crash between durable message
  append, delivery-attempt persistence, and acknowledgement must not produce
  a false caught-up result.
- A cold consumer does not trigger an unbounded log scan from a health or
  metrics request. Missing timestamp/byte index data is explicitly unknown.
- Retention tests cover `F = 0`, `protect`, `expire` with `C < F`, unavailable
  history, acknowledged-history replay, and the separation of physical bytes
  from logical lag bytes.

### Stage 2 — fixed-cardinality server metrics

- A real broker process exposes the additive aggregate families with correct
  gauge/counter types, base units, HELP text, and no caller-controlled labels.
- Tests assert that unknown, stale, truncated, and retention-expired values
  are not emitted as fresh zeroes; scrape availability, age, coverage, and
  failure counters remain visible.
- Scraping during a stalled engine remains bounded by the existing health
  timeout and preserves process/admission metrics. Readiness remains
  independent of high or unknown application lag while still failing on the
  existing engine-health timeout.
- Output bytes, concurrent lag work, snapshot item count, state reads, and
  any configured identity allowlist are bounded under consumer/name churn.

### Stage 3 — clustered aggregation

- Three real broker processes verify one logical stream is counted once, even
  when all RF=3 replicas are present and each node is scraped.
- Tests cover follower forwarding, leader change, follower staleness, leader
  loss, stream-group initialization, snapshot replacement, and partial group
  failure. A stale/unavailable source is marked with revision/status rather
  than silently reduced to zero.
- Grouped consumers verify one lag value per durable group, separate in-flight
  count, out-of-order acknowledgements, per-key blocking, expiry, fencing,
  retry, and dead-letter behavior.

### Stage 4 — optional bytes, age, and identity diagnostics

- Indexed timestamps and cumulative logical byte totals are validated across
  legacy/current formats, restart, retention cleanup, and cluster snapshot
  installation before those fields become deployment-grade metrics.
- The authenticated selected-consumer operation enforces name validation,
  deadline, response-size, and result-cardinality limits and documents every
  `fresh`/`stale`/`unknown`/`retention_expired` outcome.

## Benchmark applicability

No benchmark applies to this documentation-only proposal. A future
implementation must benchmark if it updates lag summaries on publish/poll/ack,
adds indexed storage metadata, changes health/metrics lock scope, or forwards
cluster queries. The existing benchmark suite does not by itself prove
consumer-lag overhead because its standard server metrics scrape and complete
consumer-cardinality cases are not the proposed workload.

At minimum, add a targeted diagnostic workload with empty and backlogged
streams, independent consumers, shared members, out-of-order acknowledgements,
slow/expired deliveries, variable payload sizes, metrics scraping, and
consumer churn. Measure throughput, poll/ack p50/p99/p99.9, scrape latency and
size, CPU, RSS, storage I/O, state/index work, and unknown/truncation counts.
For a cluster runtime change, use three real processes and the repository's
authoritative `just bench-pr-local` comparison against the exact recorded
`origin/main` baseline when the standard workload covers the path; otherwise
add a relevant targeted case first. Keep durability mode, message size,
membership, retention state, resource limits, and source revision in every
artifact. These measurements can establish cost or regression boundaries;
they do not permit a claim that telemetry improves broker performance.

## Recommendation

Accept this file as the TD-006 consumer-lag design proposal, keep TD-006 open,
and resolve the consumer-catalogue, retention, byte/age index, cluster-source,
and administrative-transport decisions before implementation. Implement the
record-based exact selected observation and fixed-cardinality aggregate only
after the Stage 0 contract and Stage 1/2 evidence gates are approved.
