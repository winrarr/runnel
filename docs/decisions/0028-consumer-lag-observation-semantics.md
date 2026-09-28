# ADR 0028: Consumer-lag observation semantics

- Status: accepted
- Date: 2026-09-28
- Evidence class: design/research; secondary: operability correctness
- Related: [consumer-lag telemetry design](../design/consumer-lag-telemetry.md), [TD-006](../tech-debt.md#td-006-operational-telemetry-remains-incomplete)

## Decision

Define consumer cursor lag for one logical `(stream, consumer)` as the distance
between the durable stream head `H` (exclusive) and the consumer's contiguous
durable committed offset `C`: `cursor_lag_records = H - C`, when `H >= C` and
`C` is at or above the retained floor `F`. If `C > H`, the observation is
`unknown` because the source state is inconsistent. A shared-consumer group has
one logical cursor; transient members do not have separate lag. The offset
distance is not a ready-message count.

Out-of-order acknowledged offsets at or beyond `C` do not reduce cursor lag.
They may be subtracted only for a separately named unacknowledged-record count
when the implementation can prove the complete retained range and acknowledgement
state. In-flight deliveries remain part of cursor lag and are reported
separately when their count is available. Neither cursor lag nor in-flight
count changes delivery, acknowledgement, retention, replay, dead-letter, or
readiness behavior.

An observation is `fresh` only when the head and consumer progress come from
the same source revision and meet an explicit freshness deadline. A known
last-value outside that deadline or behind the committed source is `stale`.
Missing, unreadable, incomplete, unbounded, or over-budget state is `unknown`.
If a future retained floor is above `C`, report `retention_expired` and omit
numeric lag. Unknown, stale, expired, incomplete, and absent identity state are
never represented as zero or as a caught-up consumer. A missing local
checkpoint does not establish that a durable consumer exists.

Default Prometheus metrics must not use stream, consumer, member, key, offset,
request ID, or delivery token as labels. A logical aggregate can be emitted
only for a complete declared consumer scope whose observations are fresh and
not retention-expired. Coverage and freshness are separate properties. A
partial scope must not be called broker-wide. Cluster aggregation counts each
logical data group once; replica copies are not additional consumers. A
node-local view must not be described or scraped as a logical cluster total.

This ADR accepts semantics only. It does not authorize or stabilize a Rust
engine capability, aggregate, protocol operation, metrics family, consumer
catalogue, source-revision mechanism, or freshness/coverage promise. Those
require a follow-up decision after the bounded source, consumer-coverage,
cluster query, and administrative exposure questions in the design record are
resolved. `HealthSnapshot` and the existing health response remain unchanged.

## Rationale and source comparison

The local engine persists contiguous consumer progress and separately tracks
out-of-order acknowledgements. Shared consumers persist one group cursor while
members and leases describe current delivery ownership. Therefore `H - C` is a
stable cursor-distance definition, while calling it the exact number of
unacknowledged or immediately deliverable records would misstate those states.

Reference systems support separating progress, backlog, age, and in-flight
work, but their metrics do not define Runnel's contract:

- [Apache Kafka 4.3 monitoring](https://kafka.apache.org/43/operations/monitoring/)
  describes consumer-side `records-lag` from the current offset, explicitly
  not the committed offset. Kafka's client fetch position and
  topic/partition dimensions differ from Runnel's durable server-side cursor.
- [NATS JetStream Consumer Info](https://docs.nats.io/reference/jetstream/api/consumer/info)
  separates delivered sequence, contiguous `ack_floor`, `num_ack_pending`, and
  `num_pending`. This supports distinct fields, but NATS delivery/filter
  policies and pending semantics do not prove Runnel's consumer catalogue or
  read cost.
- [Google Cloud Pub/Sub monitoring](https://docs.cloud.google.com/pubsub/docs/monitoring)
  pairs unacknowledged count with oldest-unacknowledged age and warns that
  backlog samples can have gaps for several minutes. Runnel must supply its
  own freshness state; its durable cursor distance is not Pub/Sub's backlog
  count.
- [Prometheus instrumentation](https://prometheus.io/docs/practices/instrumentation/)
  explains the per-label-set RAM, CPU, disk, and network cost, cautions against
  high-cardinality identity labels, and recommends exporting Unix timestamps
  rather than time-since gauges. Runnel's names are caller supplied, so they
  remain outside default metric labels.

## Alternatives considered

- **Call `H - C` an unacknowledged count.** Rejected because durable
  out-of-order acknowledgements can exist above the contiguous cursor.
- **Count ready messages by scanning retained records.** Rejected because
  keyed delivery, leases, acknowledged offsets, and same-key blocking make
  eligibility a different concept and a scan is not a bounded health scrape.
- **Use the active consumer-state cache as a complete aggregate.** Rejected
  because local state is loaded lazily, the cache is capped at 1,024 entries,
  and entries can be absent after restart or eviction.
- **Emit per-consumer metric labels or dynamic top-K labels.** Rejected as the
  default because identity churn creates unbounded series. Identity inspection,
  if later accepted, belongs in a bounded separate interface.
- **Sum each node's local clustered health.** Rejected because replicated
  copies would multiply one logical consumer's lag. A committed source revision
  and logical-group deduplication are required for any cluster aggregate.

## Consequences and follow-up gates

- `cursor_lag_records`, in-flight records, unacknowledged records, age, and
  logical bytes have distinct meanings. Age and byte values require bounded
  indexed metadata; physical storage bytes are not a substitute.
- Current retention is unlimited, so the current retained floor is `F = 0`.
  The expired-history outcome applies when a future retention policy advances
  that floor; it must not silently clamp `C` to `F`.
- Local checkpoint files are not size-bounded even though their journals are
  limited to 64 KiB. The cache limit does not bound cold-read bytes. A runtime
  query must cap local read/parse work or use a bounded summary and return
  `unknown` when that source cannot meet the cap.
- The current cluster inspection path returns no applied source revision, and
  health sums node-local materialized groups. A future clustered query must
  establish its committed revision, bounded forwarding/deadline, and
  replica-deduplication behavior.
- Before implementation, choose how known consumers are distinguished from
  arbitrary valid names; whether to create a complete bounded catalogue or a
  limited scope; and the exposure, authorization, compatibility, concurrency,
  deadline, and response-byte policies.
- Real-process tests must cover restart, missing/oversized state, unknown and
  stale observations, incomplete coverage, retention expiry, scrape bounds,
  leader changes, and three-node replica deduplication. A runtime change that
  updates durable summaries or query paths also needs crash/recovery evidence
  and a relevant targeted benchmark.
- TD-006 remains open. This decision adds no metrics or runtime telemetry and
  does not create a separate backlog item; bounded source access and coverage
  remain gates under that existing observability outcome.
