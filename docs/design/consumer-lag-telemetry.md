# Consumer-lag telemetry design

- Status: semantics accepted by
  [ADR 0028](../decisions/0028-consumer-lag-observation-semantics.md); the
  first bounded operator observation is accepted by
  [ADR 0041](../decisions/0041-first-consumer-lag-observation.md); runtime is
  not implemented
- Last reviewed: 2026-10-06
- Baseline inspected: `a77b8e0fcc0b10c8adf33e0c7e4cb34dcdc49a0`
- Evidence class: design/research; secondary: operational telemetry
- Related debt: [TD-006](../tech-debt.md#td-006-operational-telemetry-remains-incomplete)
- Scope: bounded logical consumer-lag observation for local and early clustered engines

This is design-only, not an implementation or a runtime guarantee. Rust code
and tests remain authoritative if this note becomes stale.

The accepted contract defines logical cursor lag and its unavailable states.
ADR 0041 selects one operator-authorized, exact-identity protocol-v2
diagnostic with explicit local and clustered read bounds. It does not add an
engine capability, runtime API, or metric family to the current code. Any
future implementation must keep diagnostic work bounded, avoid
caller-controlled Prometheus labels, and count a replicated consumer once.
`HealthSnapshot` and the existing health response remain separate from this
optional telemetry.

## Current baseline

The inspected baseline still has no consumer-lag API or metric family. Its
current signals establish adjacent state, but do not report `H - C` or a
complete consumer scope:

- [`runnel_engine::HealthSnapshot`](../../crates/runnel-engine/src/lib.rs#L178)
  contains broker-wide stream count, storage bytes, in-flight deliveries,
  redelivery count, and dead-letter count. The protocol health response
  exposes only stream count and storage bytes. The engine's
  [`inspect_consumer`](../../crates/runnel-engine/src/lib.rs#L397) contract
  returns a `ConsumerPolicy`; local
  [`Broker::inspect_consumer`](../../crates/runnel-core/src/broker.rs#L271)
  reads retry policy, not consumer progress. `ConsumerPolicy.version` is not
  an applied Raft log revision. Neither operation is a consumer-lag source.
- The server's [`/metrics` handler](../../crates/runnel-server/src/observability.rs#L344)
  exports fixed operation labels and aggregate request, traffic, publish,
  delivery, acknowledgement, admission, storage, health, redelivery,
  dead-letter, and clustered snapshot signals. In particular,
  `runnel_in_flight_deliveries` is the health snapshot's current tracked
  delivery count. It is not cursor lag, ready work, or the server's separate
  in-flight request count. The [health response](../../crates/runnel-server/src/observability.rs#L305)
  remains separate. Cluster snapshot counters are collected after the bounded
  health call; that second query has no independent explicit deadline.
- The [protocol listener](../../crates/runnel-server/src/protocol.rs) has
  configurable request-frame and end-to-end request limits, and the [peer
  forwarder](../../crates/runnel-raft/src/forwarding.rs) has a retry timeout.
  The JSON-lines protocol has no authentication or negotiated runtime
  handshake. The [framed peer listener](../../crates/runnel-raft/src/network/inbound.rs)
  also has no authentication. The [HTTP listener binding](../../crates/runnel-server/src/bootstrap.rs)
  exposes `/metrics` without authentication and can bind beyond loopback when
  configured. The protocol's [response limit](../../crates/runnel-protocol/src/lib.rs#L81)
  is payload-oriented (65 MiB in the client), and the server has no smaller
  limit for a fixed diagnostic response. These existing controls do not
  authorize an identity-selected consumer query.
- Local [`Broker::health`](../../crates/runnel-core/src/broker.rs#L497)
  sums stream-log file lengths and the transient delivery index while holding
  each stream lock. The local in-flight index is process memory and is rebuilt
  empty after restart; expired entries leave it when a later poll or
  acknowledgement runs, not through a background expiry worker. The durable
  checkpoint separately stores the contiguous `committed_offset`,
  out-of-order acknowledgements, delivery attempts, configured policy, and
  policies pinned to offsets. Consumer state is loaded lazily, with no complete
  durable consumer catalogue. The cache limit of 1,024 entries does not bound
  checkpoint file size or identify all consumers. Journal writes have a
  1 MiB cap, checked after reading during recovery; checkpoint files and their
  acknowledgement sets have no equivalent bound. Local redelivery and
  dead-letter totals are process atomics initialized at open; clustered totals
  come from replicated state-machine state, so restart behavior differs by
  engine.
- Clustered [`GroupManager::health`](../../crates/runnel-raft/src/group_manager.rs#L633)
  sums materialized groups on the current node. A data-group health snapshot
  derives `storage_bytes` by iterating retained messages and summing logical
  key-plus-payload bytes, and counts its applied in-flight lease state. This
  is not a leader-authoritative cross-node lag query and does not deduplicate
  replica copies across node scrapes. Clustered policy inspection uses the
  stream's current leader, but returns no applied source revision. A local
  stream-log file length and clustered logical payload-byte sum are different
  storage measurements; neither defines consumer-lag bytes, and
  `runnel_storage_bytes` must not be repurposed as such.
- The Raft state machine persists an applied `LogId` with its snapshot and
  journal, but current policy inspection returns no such revision. Existing
  clustered inspection selects or forwards to the current leader, then reads
  state without a lag-specific read barrier or applied-revision result. A
  leader check alone is not evidence that a sampled head and cursor correspond
  to a fresh committed revision.
- Retention remains unlimited (`F = 0`) and replay is an explicit-offset,
  read-only operation. The [retention design](retention-disk-pressure-plan.md)
  describes proposed `protect`/`expire` behavior; those are not current
  guarantees.

The existing evidence is about durable progress, leases, and scrape behavior,
not about lag computation. These exact tests show the current boundaries:

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| [`grouped_consumers_share_records_and_allow_out_of_order_acknowledgements`](../../crates/runnel-core/src/lib.rs#L714) and [`acknowledged_group_progress_and_retry_state_survive_restart`](../../crates/runnel-core/src/lib.rs#L2111) | Local shared-group acknowledgements can be out of order; acknowledged progress and unacknowledged delivery survive restart. | A lag value, catalogue completeness, or bounded telemetry read. |
| [`acknowledged_consumer_state_cache_is_bounded`](../../crates/runnel-core/src/lib.rs#L307), [`consumer_delivery_journal_stays_within_its_checkpoint_bound`](../../crates/runnel-core/src/lib.rs#L1988), and [`oversized_consumer_delivery_journal_is_rejected_on_recovery`](../../crates/runnel-core/src/lib.rs#L2028) | The in-memory cache and journal have explicit limits, and an oversized journal is rejected. | A bound on checkpoint bytes: the test covers the journal, not the checkpoint file or its out-of-order acknowledgement set. |
| [`health_reports_in_flight_deliveries_until_acknowledged`](../../crates/runnel-core/src/lib.rs#L757) and [`health_reports_in_flight_deliveries_until_group_acknowledged`](../../crates/runnel-raft/src/lib.rs#L1738) | The health snapshot's in-flight count changes across delivery and acknowledgement in local and clustered state-machine tests. | Durable cursor lag or a fresh, deduplicated three-node aggregate. |
| [`metrics_report_messages_returned_by_polls`](../../crates/runnel-server/tests/server_smoke.rs#L347) | A real server process exposes the aggregate in-flight delivery gauge and delivery/ack counters across publish, poll, and ack. | Consumer identity, cursor distance, or a lag family. |
| [`metrics_report_protocol_failures_without_stream_labels`](../../crates/runnel-server/tests/server_smoke.rs#L519) | Request metrics use fixed operation labels and do not expose caller stream or consumer names on this real-server path. | Complete coverage or lag-series behavior; the current endpoint has no per-consumer series. |
| [`storage_stall_is_bounded_and_durable_traffic_continues`](../../crates/runnel-server/tests/admission.rs#L1788) and [`sustained_in_flight_pressure_reports_metrics_and_recovers`](../../crates/runnel-server/tests/admission.rs#L666) | Real-process tests cover scrape fallback during a stalled engine health call and report request-admission pressure and recovery. | A bound for optional lag collection or for the separate clustered snapshot-metrics call. |
| [`grouped_lease_has_no_lazy_expiry_without_a_committed_command`](../../crates/runnel-raft/src/lib.rs#L767) | Clustered lease expiry follows committed state transitions rather than a background read-time cleanup. | Fresh lease counts during inactivity or a lag freshness policy. |

There is no current lag-specific test. Existing local and clustered tests can
support a future semantic implementation, but the selected-observation,
unknown-state, scrape-bound, freshness, coverage, and three-node
replica-deduplication cases remain unimplemented. Existing
`runnel_in_flight_deliveries` must therefore be described as tracked delivery
state only, not as a consumer-lag proxy.

[`TD-006`](../tech-debt.md#td-006-operational-telemetry-remains-incomplete) remains open because the current signals cannot explain consumer lag,
retained or reclaimable storage, queue saturation, replication progress, or
resource pressure. This document defines one accepted semantic slice of that
outcome; it does not retire the debt item or authorize runtime behavior.

## Accepted semantics and open runtime design

[ADR 0028](../decisions/0028-consumer-lag-observation-semantics.md) accepts
the meaning of cursor lag, the distinction between lag and in-flight work, and
the rule that unavailable or incomplete data is never represented as zero.
[ADR 0041](../decisions/0041-first-consumer-lag-observation.md) selects the
first exact-identity operator operation, its v2/operator authorization path,
its local byte caps, freshness rule, and the required OpenRaft barrier and
applied revision. These are accepted target semantics, not implemented
runtime guarantees. Aggregation, metric families, and a consumer catalogue
remain unaccepted and unimplemented.

## Semantics

Lag must identify which notion of progress it measures. The accepted model uses
the following values for one logical `(stream, consumer)`:

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

The selected diagnostic field is `cursor_lag_records`; a future aggregate may
use `runnel_consumer_lag_records` only when its declared consumer scope is
complete. Calling `H - C` an "unacknowledged count" would be wrong for grouped
consumers because out-of-order acknowledgements are allowed. The separate
`unacknowledged_records` field can be added only when the implementation proves
that its retained-range accounting is complete.

The following rules are part of the accepted contract:

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

If introduced, lag should be a separate, optional engine telemetry capability
rather than an extension of `HealthSnapshot`. It would return a bounded
`ConsumerLagSnapshot` with an observation timestamp, source revision, coverage
status, and either aggregate values or one explicitly selected consumer. An
unsupported or incomplete source must yield `unknown`; it must not fabricate
zeroes. This capability shape is an implementation option, not part of ADR
0028.

### Accepted first exact-identity observation

[ADR 0041](../decisions/0041-first-consumer-lag-observation.md) accepts one
validated `(stream, consumer)` request over negotiated protocol v2, authorized
for the operator role. It returns one fixed-shape result from one consistent
local or leader-authoritative data-group sample. The response carries status,
an optional `cursor_lag_records`, the sample time, and an opaque equality-only
source revision. It does not echo the identity. A fixed reason code explains
`unknown`; no lag value accompanies an unavailable or incomplete source. The
first slice omits age, logical bytes, unacknowledged counts, and in-flight
counts. The result shape is:

```text
status = fresh | unknown | retention_expired
reason = missing_state | incomplete_state | over_budget | busy | source_unavailable
cursor_lag_records (present only for fresh)
observed_at_ms (present when a complete sample exists)
source_revision (opaque and equality-only; present when a complete sample exists)
```

The response is capped at 2 KiB. `stale` is not emitted because this slice
never serves a cached result. If a supported retention floor later establishes
`F > C`, return `retention_expired`, omit numeric lag, and include `F`, `C`,
and `H` as required by ADR 0028. No runtime endpoint is implemented.

A missing durable state entry is not proof that a named consumer exists or is
caught up. A valid durable checkpoint or replayable journal locally, or a
replicated consumer entry in the stream group, establishes known state. A
policy-only entry counts. Absence returns `unknown` with reason
`missing_state`, no numeric lag, and no mutation. This rule intentionally
treats empty-poll history as engine-specific: a local empty poll does not
persist state, while a clustered group poll initializes replicated consumer
state. Neither path may turn absence into zero.

### Local engine

The recovered local stream log exposes its exclusive `next_offset` as `H`;
the consumer checkpoint stores `C` and out-of-order `A`, while the active
delivery index supplies a current process-local count. The existing
`inspect_consumer` path returns retry settings and is not a progress query.
There is no current telemetry operation that reads these values together.
The accepted query samples `H`, `C`, and the retained floor under the existing
per-stream lock and never enumerates log records. Its future diagnostic-only
reader is capped at 1 MiB each for the checkpoint and journal (2 MiB total plus
two cap-detection bytes), without changing ordinary checkpoint recovery.
Over-cap, corrupt, or incomplete files yield `unknown` without repair or
truncation. The baseline maximum 1,024-record batch fixture measured 123,854
journal bytes and a 101,348-byte checkpoint projection, so 64 KiB would reject
a valid single batch; larger multi-member state can still exceed the selected
cap. The local source revision identifies this same lock-consistent sample and
is opaque and equality-only. Looking up an old message timestamp is a separate
indexed-metadata operation and is not available from the current tail/sparse
indexes with a guaranteed bounded cost.

The current local state files are authoritative but are discovered lazily and
there is no complete consumer catalogue. Broker-wide aggregates therefore
cannot be complete using current code. A future aggregate requires one of
these choices:

- add a durable per-stream consumer catalogue/summary with a validated
  configured maximum and a bounded recovery path; or
- define a bounded configured scope and report coverage for that scope; it
  must not call the result broker-wide or claim complete coverage while other
  durable consumers may exist.

Neither choice is accepted yet. An active-state cache is not a catalogue; a
directory walk or full checkpoint scan during `/metrics` is not an acceptable
substitute. A maximum would affect implicit consumer creation, and the current
protocol has no consumer-delete operation. A configured scope is useful only
if it can be observed without labels or per-identity unbounded work.

If a summary is later added, maintain it from durable state transitions and
rebuild it once at startup from bounded metadata. A partial or failed rebuild
must expose `unknown`/`truncated`, not a partial result described as the
broker-wide total. Persistence of derived lag metadata is a storage change and
requires crash/recovery and compatibility evidence.

### Clustered engine

The static cluster has one logical data group per stream and replicates
consumer progress and grouped in-flight state in that group. The accepted exact
query resolves the stream's data group and reads only from its current leader.
That leader confirms a linearizable barrier, then samples stream head, durable
consumer progress, retained floor, and `last_applied_log` under one state
read-lock operation. It returns a numeric result only when the sample's applied
revision covers OpenRaft's barrier. No cached result is accepted in this first
slice; barrier, apply, initialization, leadership, or forwarding failure
returns `unknown` without a numeric value.

`GroupManager::health` is currently a local-node aggregate. It is useful as a
broker health signal but must not become a cross-node lag reducer: summing the
same stream's RF=3 state from three node scrapes would triple the logical lag.
The preferred cluster metric path is a leader-authoritative logical aggregate
served from any node through a bounded forward/query. If that is not available,
do not emit a cluster-wide aggregate. A node-local metric must say that it is
node-local, and dashboards must select one source rather than summing replica
scrapes. A follower's cached value may be used only with `stale` status and a
source revision, never as a fresh committed cluster result.

The metadata group must not be mistaken for a consumer-bearing stream group.
An exact query returns one answer for the selected stream and does not enumerate
or sum replica copies. Internally, aggregate work must key each result by
stable logical stream/data-group identity. A cluster-wide aggregate cannot be
emitted by every node's `/metrics` endpoint and then summed: it needs one
designated collector or an external deduplicator. Until that exists, clustered
lag is not exported as a metric.

## Metrics and cardinality

If a future `/metrics` implementation can produce a complete, fresh logical
aggregate, it should add only fixed-cardinality families. The suggested names
are explicit about aggregation and units:

| Metric | Type | Definition |
| --- | --- | --- |
| `runnel_consumer_lag_records` | Gauge | Sum of `cursor_lag_records` over all covered logical consumers; omit when coverage is incomplete |
| `runnel_consumer_lag_max_records` | Gauge | Maximum `cursor_lag_records` over covered consumers; omit when coverage is incomplete |
| `runnel_consumer_lag_oldest_unacknowledged_timestamp_seconds` | Gauge | Oldest known pending record timestamp; omit when no complete timestamp sample exists |
| `runnel_consumer_lag_consumer_count` | Gauge | Number of logical consumers included in the snapshot |
| `runnel_consumer_lag_coverage_complete` | Gauge | `1` when all consumers in the declared scope were observed; `0` for partial/truncated coverage |
| `runnel_consumer_lag_unknown_consumers` | Gauge | Known consumers whose source/status is unknown in this snapshot |
| `runnel_consumer_lag_expired_consumers` | Gauge | Known consumers behind the retained floor |
| `runnel_consumer_lag_snapshot_available` | Gauge | `1` only when the aggregate snapshot is fresh and complete; otherwise `0` |
| `runnel_consumer_lag_snapshot_timestamp_seconds` | Gauge | Unix timestamp of the last completed snapshot |
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

No default metric should have `stream`, `consumer`, `member`,
`offset`, `key`, payload, request ID, or delivery token labels. The current
server convention already avoids caller-controlled stream and consumer
labels. Prometheus recommends minimal labels and warns that user or resource
identities can create unbounded time-series cardinality; future default metrics
must keep that boundary even though per-consumer lag is useful.

If future operations need identity-labelled metrics, require a static,
validated operator allowlist with an explicit maximum series budget. Do not
implement dynamic top-K labels as the default: churn can create unbounded
historical time series even when the current scrape contains only K entries.
An allowlist would use a separate metric family from the aggregate and would
need a bounded exposition byte limit, fixed identity count, and tests for
series eviction/configuration changes. Until then, selected-consumer
diagnostics are the identity-bearing surface.

The collector must not allocate one task, timer, metric child, or payload copy
per consumer or record. A selected query may accept at most one validated
`(stream, consumer)` identity and return one fixed-shape record. Its source
read must have explicit byte, parse-work, concurrency, and deadline limits; a
response-size limit alone is insufficient. A future aggregate must have hard
limits for consumer identities covered, groups visited, source bytes read,
exposition bytes, concurrent work, and deadline. The exact numeric ceilings
remain open until the source representation and representative workloads are
measured. Exceeding any bound is `truncated` or `unknown`, never a partial
broker-wide value; it must not block publish, poll, acknowledgement, shutdown,
or ordinary health.

## Freshness, unknown state, and health interaction

The following status meanings are accepted; freshness budgets and source
mechanisms remain open runtime decisions:

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

The existing one-second timeout bounds the `engine.health()` portion of
readiness and metrics. In the current `/metrics` path, clustered
`snapshot_metrics()` collection runs after that health check and has no
separate explicit deadline; this is existing snapshot-telemetry behavior, not
a consumer-lag guarantee. A future lag collector must have its own bounded
work/deadline policy and must not lengthen the health check or make readiness
depend on application backlog. `/health/ready` should continue to mean that
the broker can serve its declared durable workload: a large lag, an expired
consumer, or an unavailable optional lag observation does not by itself make
the broker unready. An engine health timeout still makes readiness fail and
should keep the current `runnel_health_check_failures_total` behavior. A lag
query that times out should use its own bounded failure/unknown result, not
turn the health response into a false healthy zero.

For predictable scrape latency, a future implementation should prefer an
atomically published/cached summary for aggregate metrics. A named query may
use a bounded engine operation, but it must have a deadline no greater than
the caller's request budget and must not consume the reserved health/shutdown
capacity. If a cache is used, export its age and source revision so operators
can distinguish service health from telemetry freshness.

## Compatibility and non-effects

The accepted semantics do not change current behavior:

- Do not add fields to `HealthSnapshot`, the existing protocol `Health`
  response, or `Message`. Existing health fixtures and clients remain valid.
- Keep the current meanings of `runnel_storage_bytes`,
  `runnel_in_flight_deliveries`, redelivery/dead-letter counters, request
  metrics, and snapshot metrics. Consumer lag is not a reinterpretation of any
  existing gauge.
- Do not expose physical log paths, Raft groups, replica placement, sparse
  indexes, or offsets as new ordinary client concepts. Any selected diagnostic
  operation needs an explicitly accepted administrative and compatibility
  boundary; it is not part of the provisional messaging contract by default.
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
| [Apache Kafka 4.3 consumer metrics](https://kafka.apache.org/43/operations/monitoring/) | `records-lag` and `records-lag-max` are consumer-side metrics based on the current offset, explicitly not the committed offset. | Runnel's durable server-side checkpoint is the progress source, and `H - C` is intentionally about committed progress. Kafka's topic/partition/client dimensions and fetch position do not map to one Runnel stream with transient shared-consumer members. |
| [NATS JetStream Consumer Info](https://docs.nats.io/reference/jetstream/api/consumer/info) | Server-side `ConsumerInfo` separates the last delivered sequence, highest contiguous acknowledged `ack_floor`, `num_ack_pending`, and `num_pending`. | The field separation supports reporting progress and outstanding work as different quantities. JetStream's delivery policies, filters, and pending semantics differ; its API does not imply that Runnel can enumerate consumers or read each state within a bound. |
| [Google Cloud Pub/Sub monitoring](https://docs.cloud.google.com/pubsub/docs/monitoring) | Recommends viewing unacknowledged count with oldest-unacknowledged age, and documents that backlog samples can have gaps for several minutes. | Complementary count and age can explain different failure shapes, but Runnel must expose its own observation timestamp and unknown/stale state. Its committed cursor distance is not Pub/Sub's unacknowledged count. |
| [Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/) and [metric naming/cardinality guidance](https://prometheus.io/docs/practices/naming/) | Current state belongs in gauges; every label set consumes resources, and high-cardinality identities should be avoided. For elapsed time, export the event's Unix timestamp and derive age in queries. | Keep default metrics label-free with no stream, consumer, member, key, offset, or token values. Export snapshot and record timestamps rather than maintaining time-since gauges. Identity-selected diagnostics are a separate bounded interface, not per-consumer series. |

## Accepted first slice

[ADR 0041](../decisions/0041-first-consumer-lag-observation.md) resolves the
former Stage 0 alternatives as one bounded design target:

- Inspect exactly one validated `(stream, consumer)` through a negotiated v2
  operation available to the `operator` role. The application role, v1
  listener, HTTP routes, and `/metrics` receive no identity-selected result.
- Report `H - C` only for a known durable state from one current sample. A
  missing entry is `unknown/missing_state`, never zero, and the query must not
  create consumer state. There is no catalogue or completeness claim.
- Freshness means a request-local, uncached sample. The local engine samples
  under the stream lock; the cluster reads only from the current data-group
  leader after OpenRaft `ensure_linearizable()`, then atomically captures the
  source values and applied `LogId`. Unavailable or incomplete sources return
  unknown without lag; the first slice does not serve stale cache entries.
- The local diagnostic reader has a 1 MiB cap for each of checkpoint and
  journal files. The existing 1,024-record fixture measured 123,854 journal
  bytes and a 101,348-byte checkpoint projection, so the proposed 64 KiB cap
  was too small. Valid state beyond 1 MiB remains explicitly unknown.
- A broker admits one lag query at a time without queuing; the absolute
  deadline is at most one second and also respects the remaining request
  deadline. The serialized response is capped at 2 KiB.
- Aggregates, per-identity metric labels, age, byte lag, and unsupported source
  details remain deferred. Current retention is unlimited; if future bounded
  floor metadata establishes `F > C`, the result is `retention_expired` with no
  numeric lag and the `F`, `C`, and `H` diagnostics specified by ADR 0028.

The read barrier is supported by the pinned OpenRaft 0.9.25 primary API, but
Runnel does not yet call it from consumer inspection. Its adapter must bind the
returned barrier to an atomic state-machine sample whose `last_applied_log`
covers it and enforce one deadline across forwarding, barrier, apply wait,
read, and response. A leader hint or applied revision sampled without the
barrier is not sufficient. See ADR 0041 for the accepted wire role, reason
codes, source revision rules, and implementation gates.

### Stage-zero disposition

The design decision is complete; runtime work remains gated on protocol v2
TLS/authentication and operator authorization, a non-mutating bounded local
reader, and the barrier-backed applied-revision adapter contract. TD-006 stays
open because none of these runtime behaviors or observability outputs exists
in the current code.

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
  operations.** A direct leader query avoids summing replica copies, but
  forwarding cost and leader availability need a real cluster workload before
  accepting that read path.
- **H4 — logical byte lag is worth the extra metadata.** Variable payloads,
  out-of-order acknowledgements, retention floors, and frame-format changes
  may make record lag plus age the more robust first release.

The runtime gaps are implementation gates, not unresolved design alternatives:

1. Protocol v2, TLS/authentication, and operator-role authorization are
   accepted by ADRs 0031 and 0035 but do not exist in the current listener.
   The diagnostic must not be added to v1 or the unauthenticated HTTP routes.
2. The required read barrier is available in pinned OpenRaft 0.9.25, but the
   Runnel adapter must call it and return a same-lock applied revision with the
   selected head and cursor under one absolute deadline.
3. The local diagnostic parser must enforce the selected 1 MiB cap per file
   without changing ordinary recovery. The measured 1,024-record fixture
   exceeds 64 KiB; valid multi-member checkpoint state can exceed 1 MiB and
   then returns `unknown`.
4. Broker-wide aggregation remains deferred until a complete bounded
   catalogue and one cluster collector or explicit deduplicator are accepted.
5. Age, logical byte lag, and unsupported source detail remain out of scope
   until their metadata and semantics can be sampled within explicit bounds.

## Test and acceptance gates

This design-only change is classified as **Design or research**. No runtime
behavior or performance claim is made, so implementation benchmarks are not a
gate for this document. The future implementation should satisfy the
following staged gates.

### Stage 0 — accepted by ADR 0041

The identity scope, missing-state behavior, local file caps, freshness and
source-revision guarantees, v2 operator authorization, one-query admission,
one-second deadline, and 2 KiB response cap are selected in
[ADR 0041](../decisions/0041-first-consumer-lag-observation.md).
The 64 KiB file proposal was rejected based on the existing 1,024-record
fixture. Runtime implementation remains unstarted until its local, security,
and clustered gates below are satisfied.

### Stage 1 — exact local observation

- Unit tests cover a known consumer with an empty stream, `H = C`, positive
  `H - C`, out-of-order acknowledgements, one cursor for grouped members, and
  `H < C` as `unknown`.
- Reopen tests prove that lag is unchanged across checkpoint and journal
  recovery. A crash between durable message append, delivery-attempt
  persistence, and acknowledgement must not produce a false caught-up result.
- A query for an identity with no durable state returns `unknown` and does not
  create or persist a consumer checkpoint or truncate/repair its event journal.
- The diagnostic reads no stream records. Inputs at the accepted 1 MiB
  per-file caps complete; cap-plus-one input returns `unknown` without parsing
  beyond the limit. Standard recovery remains unchanged. The maximum batch
  fixture remains readable under the cap; larger valid state fails closed.
- The real-server query boundary enforces authentication, request deadline,
  concurrency and response-byte limits, and does not affect readiness or
  health response shape.

### Stage 2 — clustered exact observation

- Three real broker processes verify a selected query returns one logical
  stream result through leader forwarding even when all RF=3 replicas exist.
- A query returns `fresh` only after a read barrier and state-machine apply
  through the captured source revision. Leader change, no leader, initializing
  group, stale replica, and apply timeout return `unknown` with no lag value.
- Tests cover snapshot installation, follower forwarding, leader change, and
  restart. A query never asks every replica and never sums replica copies.
- If the result later feeds a metric aggregate, only a single designated
  cluster collector may emit that aggregate; per-node copies are not summed.

### Stage 3 — fixed-cardinality aggregate metrics

- Do not start this stage until a complete bounded consumer catalogue and its
  recovery semantics are accepted. The current exact-identity query does not
  provide catalogue completeness.
- A real broker process exposes aggregate families with correct gauge/counter
  types, base units, HELP text, and no caller-controlled labels. Incomplete
  coverage never produces a broker-wide numeric aggregate.
- Omit the oldest-record timestamp/age family until Stage 4 validates bounded
  timestamp metadata; Stage 3 reports record lag and coverage only.
- Unknown, stale, truncated, and retention-expired values are not emitted as
  fresh zeroes; availability, age, coverage, and failure state remain visible.
- Scraping during a stalled engine health check remains bounded and preserves
  process/admission metrics. A lag collector has an independent deadline and
  cannot change readiness or health response shape.
- Output bytes, concurrent lag work, consumer/group count, and state reads are
  bounded under identity churn.

### Stage 4 — optional bytes and age

- Indexed timestamps and cumulative logical byte totals are validated across
  legacy/current formats, restart, retention cleanup, and cluster snapshot
  installation before those fields become deployment-grade metrics.
- Future retention tests cover `F > C` and ensure `retention_expired` has no
  numeric lag. Current behavior remains unlimited retention (`F = 0`).

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

## Implementation feasibility review

The supplied `a77b8e0` source and tests support the `H - C` semantic
definition but do not yet implement the accepted operator observation:

- `Broker::inspect_consumer` validates one identity and reads its state under
  the stream lock, but returns retry policy. A missing checkpoint causes the
  loader to synthesize an empty in-memory state for that call. An empty local
  poll does not persist the consumer; delivery-attempt, ack, and policy events
  do. The checkpoint stores acknowledgement, attempt, and pinned-policy maps
  without a file-size limit; journal writes are capped at 1 MiB, but recovery
  reads before checking that cap. The 1,024-entry cache is a performance cache,
  not a catalogue or an on-disk size cap. The accepted diagnostic cap is 1 MiB
  per file, based on the measured maximum-batch fixture.
- The Raft state machine already persists `last_applied_log` with state
  snapshots and journals in [`StateMachineData`](../../crates/runnel-raft/src/state_machine.rs#L214).
  `group_consumers` and `consumers` contain replicated per-stream progress,
  and group polling may create state even for an empty result. Current
  clustered [`PersistentEngine::inspect_consumer`](../../crates/runnel-raft/src/engine.rs#L1069)
  selects/forwards to the current leader and returns policy only. It neither
  establishes a linearizable read barrier nor returns the applied `LogId` with
  the sampled values. The pinned OpenRaft API provides `ensure_linearizable()`;
  Runnel must add the adapter call and atomically sample a covering applied
  revision with the requested state.
- Cluster `health` is computed from each node's materialized groups. With one
  logical data group per stream replicated to all voters, summing metrics from
  each node would count one consumer multiple times. The accepted exact query
  avoids this by reading the requested stream once through its leader; a
  cluster aggregate still needs one collector or deduplication layer.
- `/metrics` bounds the `engine.health()` call at one second, then collects
  clustered snapshot metrics without an independent deadline. The protocol
  listener has a configurable request deadline and frame cap, but no
  authentication or runtime version negotiation. The HTTP metrics listener
  is also unauthenticated and configurable to bind outside loopback. Neither
  is an approved surface for identity-selected consumer state.
- Existing local and clustered tests cover cursor persistence, out-of-order
  acknowledgements, shared delivery, restart, health gauges, and metrics
  fallback, but none assert consumer lag, unknown-vs-zero, catalogue
  completeness, source revision, telemetry read bounds, or replica-deduplicated
  observation. No lag-specific benchmark exists.

Existing benchmark workloads include poll/ack paths, but do not measure a lag
query, complete consumer-scope scrape, or summary-maintenance cost. The
concrete workload and measurements required before a runtime performance
claim are listed under [Benchmark applicability](#benchmark-applicability).

The accepted first runtime slice is the exact operator-authorized
`cursor_lag_records` diagnostic for one `(stream, consumer)` in ADR 0041. It is
not safe to expose until the negotiated-v2 TLS/authentication implementation,
the diagnostic-only bounded parser, and the linearizable applied-revision
adapter are implemented and pass the gates above. A missing entry remains
unknown. A process-local cache, partial scan, per-node replica sum, `H - C`
represented as ready or unacknowledged work, or a fresh-looking zero violates
ADR 0028.

## Recommendation

Keep TD-006 open while implementing the bounded exact-identity diagnostic as
its first consumer-progress slice. ADR 0041 accepts the exposure and source
contract, but no runtime capability, API, or metric exists yet. Keep aggregate
metrics deferred until a complete bounded catalogue and one logical cluster
collector exist. Keep bytes and age out until indexed source metadata is
available and bounded. No runtime test or performance claim applies to this
design-only change; the listed implementation tests and benchmark gates apply
when runtime work begins.
