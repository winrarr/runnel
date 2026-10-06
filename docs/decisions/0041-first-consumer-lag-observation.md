# ADR 0041: Accept the first consumer-lag observation contract

- Status: accepted
- Date: 2026-10-06
- Baseline: `a77b8e0fcc0b10c8adf33e0c7e4cb34dcdc49a0`
- Primary evidence class: design/research; secondary: operability correctness
- Related: [ADR 0028](0028-consumer-lag-observation-semantics.md), [ADR 0031](0031-protocol-v2-contract.md), [ADR 0032](0032-static-cluster-peer-mutual-tls.md), [ADR 0035](0035-first-application-client-security.md), [consumer-lag telemetry design](../design/consumer-lag-telemetry.md), [TD-006](../tech-debt.md#td-006-operational-telemetry-remains-incomplete), [single-node deployment backlog](../backlog.md#make-the-single-node-deployment-ready-for-real-use)

## Context

[ADR 0028](0028-consumer-lag-observation-semantics.md) defines logical cursor
lag as the durable stream head `H` minus a known consumer's contiguous durable
cursor `C`. It intentionally leaves source completeness, observation
freshness, local read bounds, cluster reads, and operator exposure for a later
decision. The current implementation has no consumer-progress operation.
Local consumer state is loaded lazily from a checkpoint and event journal;
the checkpoint can grow with pending delivery state, while the journal is
bounded to 1 MiB by writes. Clustered progress lives in the stream's Raft data
group, and its persisted state includes `last_applied_log`, but current reads
do not combine that revision with the sampled head and cursor.

The existing JSON-lines v1 application listener and HTTP operations listener
are unauthenticated. [ADR 0035](0035-first-application-client-security.md)
accepts operator and application bearer roles over negotiated v2, with TLS
before credentials, and keeps the cleartext HTTP listener a separate,
deployment-restricted surface. An identity-selected progress read must use
that accepted authorization boundary rather than exposing caller-selected
state through `/metrics` or v1.

The inspected baseline is `a77b8e0fcc0b10c8adf33e0c7e4cb34dcdc49a0`, which
includes the existing maximum consume-batch test. A temporary read-only probe
of that fixture with 1,024 assigned records measured a 123,854-byte journal
and a 101,348-byte serialized checkpoint projection. The projection is the
same `ConsumerState` value written by checkpoint persistence, measured before
its final newline. Both are larger than the proposed 64 KiB diagnostic cap;
the journal writer itself permits up to 1 MiB. No tracked runtime code was
changed for this measurement.

## Decision

Accept one bounded operator diagnostic for one exact `(stream, consumer)`
identity. This is a future negotiated-v2 operation, not an implementation
authorization for the currently unauthenticated listeners.

### Identity, semantics, and result

- The request names exactly one validated stream and one validated consumer,
  using the existing name rules. A shared-consumer group is one identity;
  member names are not accepted. The request reads no other consumer and
  makes no catalogue or aggregate-coverage claim.
- A consumer is known only when complete, valid durable state exists for that
  identity: a local checkpoint or replayable event journal, or a replicated
  consumer entry in the stream's data group. A current grouped-consumer entry
  is authoritative; if it is absent, a legacy replicated scalar cursor in the
  data group's `consumers` map is valid state and supplies `C`. A policy-only
  entry counts as known. A missing state entry returns `unknown` with reason
  `missing_state`, no numeric lag, and no creation or persistence of consumer
  state. If the
  point-in-time source was otherwise complete, its observation time and
  opaque source revision may accompany this result.
- For a known consumer, report `cursor_lag_records = H - C` only when
  `H >= C` and `C` is not below the retained floor `F`. Out-of-order
  acknowledgements and in-flight deliveries are not subtracted. This is
  cursor distance, not ready work or an exact unacknowledged count.
- `fresh` means the value was sampled for this request from one consistent
  source state, with no cache. It does not promise that concurrent writes
  cannot occur after the sample. Return the sample's `observed_at_ms` and an
  opaque, equality-only source revision; do not expose raw revision fields as
  a new application concept. This first operation never returns `stale`.
- Return `unknown` with a fixed reason code and no numeric lag for missing,
  malformed, over-budget, incomplete, inconsistent, busy, or unavailable
  sources. Reasons are limited to `missing_state`, `incomplete_state`,
  `over_budget`, `busy`, and `source_unavailable`; diagnostics must not
  include file paths, acknowledgement sets, delivery tokens, or raw cluster
  layout. A source revision is present only when a complete point-in-time
  sample (including a confirmed missing identity) exists.
- If a supported retention floor later establishes `F > C`, return
  `retention_expired`, omit numeric lag, and include `F`, `C`, and `H` as
  required by ADR 0028. Current retention is unlimited (`F = 0`), so this
  result is not expected today. No other raw offsets are part of the response.
- The response contains only status, the bounded reason when applicable,
  optional lag, sample time, opaque source revision, and the three offsets for
  `retention_expired`. It is capped at 2 KiB serialized. No consumer identity
  is echoed by the response; it is already present in the request.

### Local source and byte budget

- Hold the existing per-stream lock while sampling the recovered exclusive
  stream head and the selected consumer cursor. Do not scan retained messages,
  enumerate consumer files, update the delivery-state cache, repair or
  truncate a journal, or create missing state.
- Use a diagnostic-only read path. Read at most 1 MiB plus one byte from each
  of the checkpoint and journal, for a total source-file budget of 2 MiB plus
  two detection bytes per query. Parse no more than 1 MiB from either file.
  Exceeding either cap, invalid state, or a partial/corrupt journal returns
  `unknown` and leaves the files untouched. Normal recovery behavior is not
  changed. This cap bounds bytes, not the full in-memory expansion of parsed
  state; the single-query concurrency limit below bounds simultaneous parser
  work.
- The 64 KiB proposal is rejected: the existing 1,024-record consume-batch
  fixture needs 123,854 journal bytes and a 101,348-byte checkpoint
  projection. The selected 1 MiB per-file cap matches the journal's current
  write bound and leaves more than ten times the measured one-batch state
  size. Larger valid checkpoints can still exceed this limit and must report
  `unknown`; a future summary or format bound would need its own decision and
  recovery evidence.
- Run filesystem reads on the bounded storage executor. A request timeout
  does not release the diagnostic permit while non-cancellable blocking I/O
  is still running. The parser must enforce the byte limits before allocating
  or parsing beyond them and must not call the ordinary recovery loader,
  which can repair a journal tail.
- The local source revision identifies the same lock-consistent `(H, C, F)`
  observation. Its representation is owned by the engine, is opaque and
  equality-only to the protocol, and need not be durable across process
  restart.

### Cluster source revision and forwarding

- Resolve one stream to its one logical data group and read only from the
  current leader. A node receiving the client request may serve the read when
  it is leader or forward one bounded peer request to that leader. Do not
  sample followers or sum replica copies. Internal forwarding uses the
  authenticated peer channel accepted by ADR 0032 and does not forward the
  client's bearer token.
- The adapter must call the pinned OpenRaft 0.9.25
  [`Raft::ensure_linearizable`](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.ensure_linearizable)
  on the selected data group. OpenRaft documents this method as confirming
  leadership with a quorum and waiting for its read log ID to be applied.
  Afterward, one `StateMachineStore` read-lock operation must return the
  stream head, consumer cursor/state presence using the current-over-legacy
  precedence above, retained floor, and the same state's `last_applied_log`.
  The adapter accepts the sample only when
  that applied revision is at or beyond the barrier returned by OpenRaft; the
  applied `LogId` is the cluster source revision. The OpenRaft barrier exists
  in the pinned primary API, but Runnel's current adapter does not use it for
  reads or return an applied revision with consumer state.
- `ensure_linearizable()` has no caller-supplied timeout. The Runnel adapter
  must apply the request's single absolute deadline around leader forwarding,
  barrier establishment, apply wait, state sampling, serialization, and
  response writing. The barrier may not be skipped, restarted with a fresh
  timeout, or replaced by a leader hint or a local `last_applied_log` check.
  If the barrier cannot be established, apply does not cover it, leadership
  changes, the group is initializing, or the deadline expires, return
  `unknown`/`source_unavailable` (or close when no response budget remains)
  with no numeric lag. Do not fall back to a follower or cached value.
- If the OpenRaft call cannot be safely cancelled at the deadline in the
  adapter's runtime, the adapter must provide an equivalent bounded
  cancellation-safe read operation before this feature is implemented. It
  may not claim that timing out a caller alone bounded the underlying work.

### Exposure and resource limits

- Add the operation only to negotiated protocol v2, behind an optional
  `consumer_progress_inspection` capability per ADR 0031. The `operator` role
  may invoke it; the `application` role may not. Authorization runs before
  engine dispatch and a denied request reveals no stream or consumer
  existence. This classifies the new operation under ADR 0035's two-role
  model without adding per-stream ACLs or another credential type.
- Do not add a v1 JSON-lines operation, HTTP route, query parameter, `/metrics`
  sample, or identity-bearing metric label. Do not expose the result through
  readiness or liveness. The accepted HTTP listener remains separate and
  deployment restricted. A future HTTP management surface would require its
  own authentication and authorization decision.
- Admit one consumer-progress query at a time per broker with a dedicated
  permit and no waiting queue. The same limit applies to locally served and
  forwarded inbound queries. A busy request returns bounded `unknown` with
  reason `busy`; it does not consume health, readiness, or ordinary request
  admission capacity.
- Cap the total query deadline at one second and at the remaining v2 request
  deadline, whichever is shorter. The absolute deadline covers local file
  reads, cluster forwarding, the OpenRaft barrier, state-machine apply and
  sample, encoding, and response writing. Forwarded retries cannot reset it.
  The one-second bound and 2 KiB response cap are resource limits, not latency
  SLOs or performance claims.
- Do not add aggregate metrics, per-identity labels, age lag, byte lag,
  unacknowledged counts, or in-flight fields in this slice. A future aggregate
  needs a complete bounded consumer catalogue and one logical cluster
  collector; age and byte lag need bounded indexed metadata and separate
  semantic decisions.

## Rationale and source comparison

The exact identity scope avoids claiming complete consumer coverage when local
files are discovered lazily. It also avoids per-consumer Prometheus series:
Prometheus instrumentation guidance notes that every unique label set creates
a time series and recommends alternatives when dimensions can grow large.
Kafka's consumer `records-lag` is published on clients and is based on current
fetch position rather than committed offset, while JetStream reports its
acknowledgement floor, outstanding acknowledgements, and pending records as
separate fields. These references support keeping Runnel's durable cursor
distance distinct from ready or in-flight work; their consumer models do not
establish Runnel's state coverage or storage-read bounds.

OpenRaft 0.9.25 directly supports the required cluster barrier: its read
protocol documents leader quorum confirmation, a `read_log_id`, waiting for
state-machine apply, and `ensure_linearizable()` as the combined operation.
That is stronger evidence than the current Runnel leader-routed policy read.
Runnel still needs an adapter method that samples the value and applied
revision from one state-machine read lock and applies a shared deadline; it
must not infer a barrier from `current_leader()` or `last_applied_log` alone.

Primary references:

- [OpenRaft 0.9.25 linearizable read protocol](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/read/index.html) and [`Raft::ensure_linearizable`](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.ensure_linearizable).
- [Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/) and [metric naming and label guidance](https://prometheus.io/docs/practices/naming/).
- [Apache Kafka 4.3 monitoring](https://kafka.apache.org/43/operations/monitoring/) and [NATS JetStream Consumer Info](https://docs.nats.io/reference/jetstream/api/consumer/info).
- [ADR 0031](0031-protocol-v2-contract.md), [ADR 0032](0032-static-cluster-peer-mutual-tls.md), and [ADR 0035](0035-first-application-client-security.md) for Runnel's accepted protocol, peer-authentication, and operator-role boundaries.

## Alternatives considered

- **Put identity-selected values on `/metrics`.** Rejected because it is
  currently unauthenticated, would turn caller-controlled names into metric
  dimensions or query behavior, and does not provide a complete catalogue.
- **Add a route to the JSON-lines v1 listener.** Rejected because the listener
  has no authentication and v1 has no compatibility or role-negotiation
  boundary.
- **Create a separate management listener and credential system.** Rejected
  for the first slice because ADR 0035 already accepts an operator role and
  ADR 0031 defines the negotiated application protocol. A second security
  surface would add independent authentication, configuration, and deployment
  rules without evidence that the fixed operator role is insufficient.
- **Return a node-local or cached cluster value when the leader is unavailable.**
  Rejected because it could appear fresh without a proven committed revision
  and can double-count replica state when scraped across nodes.
- **Keep the 64 KiB local file cap.** Rejected by the measured existing
  maximum-batch fixture; it would mark a valid single-batch consumer unknown.
  The 1 MiB cap is still intentionally fail-closed for larger checkpoints.
- **Add aggregate lag metrics or age/byte fields in the first slice.** Deferred
  because no bounded catalogue or validated indexed source metadata exists.

## Consequences and implementation gates

- This decision resolves the first observation contract but does not implement
  it. Current v1, HTTP, local storage, and clustered adapter behavior is
  unchanged. No runtime tests or benchmark claims apply to this documentation
  change.
- Runtime implementation must be gated on negotiated-v2 authentication and
  operator authorization; a diagnostic-only local parser with cap, no-mutation,
  and unknown-state tests; and an adapter read that proves one applied
  committed revision after OpenRaft's barrier.
- Required local tests cover empty stream with known state, caught-up and
  positive lag, out-of-order acknowledgement, grouped cursor identity, missing
  state without creation, `H < C`, corrupt and partial files without repair,
  both file caps and cap-plus-one, and restart stability.
- Required real-process protocol tests prove application-role denial before
  dispatch, operator-role success, no v1/HTTP exposure, bounded response and
  request deadlines, busy admission, and independent health/readiness behavior.
- Required three-node tests query through a follower and prove exactly one
  leader-authoritative logical result. Leader loss/change, unavailable quorum,
  group initialization, apply lag, timeout, restart, and snapshot install must
  return no numeric lag when a complete fresh sample cannot be proven.
- A later aggregate requires a complete bounded catalogue and one designated
  cluster collector. Per-identity labels remain excluded. Age and byte lag
  remain deferred until their metadata and retention behavior have separate
  bounded evidence.
- TD-006 remains open and is not partially retired by an accepted design.
  The single-node deployment backlog now records this operator diagnostic as
  the accepted first slice; metrics aggregation and the rest of deployment
  observability remain unfinished.
