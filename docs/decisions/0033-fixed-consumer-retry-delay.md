# ADR 0033: Fixed per-consumer retry delay

- Status: accepted
- Date: 2026-10-06
- Extends: [ADR 0027](0027-consumer-scoped-retry-policy.md)
- Related: [TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse) and [TD-020](../tech-debt.md#td-020-clustered-delivery-leases-use-absolute-wall-clock-deadlines)

## Context

ADR 0027 separates an acknowledgement lease from durable attempt accounting,
pins a consumer-policy snapshot on first assignment, and keeps local leases
process-local while clustered leases use replicated wall-clock deadlines. The
current implementation redelivers on lease expiry without an additional
delay. Applications therefore cannot slow repeated delivery to a dependency
that is failing, while changing `ack_timeout_ms` to achieve that also changes
how long healthy work may remain unacknowledged.

The local engine uses monotonic `Instant` deadlines that are discarded on
restart. The clustered engine persists absolute millisecond lease deadlines
and evaluates them using its persisted nondecreasing lease-clock floor. That
floor prevents a backwards observation from reversing already-observed time,
but it does not bound clock skew, wall-clock jumps, or progress without a
committed command; those remain TD-020 risks.

## Decision

Extend the durable consumer policy with one fixed `retry_delay_ms` value.
Extend `configure_consumer(stream, consumer, ack_timeout_ms,
max_delivery_attempts, retry_delay_ms)` and return it from
`inspect_consumer(stream, consumer)`, so timeout, attempt limit, and retry
delay advance atomically under one policy version. The value is bounded from
zero through seven days, matching the existing maximum for `ack_timeout_ms`.
The seven-day bound applies to each field independently, so the maximum
configured lease-plus-delay sum is fourteen days. Demand-driven expiry
observation can extend actual assignment-to-redelivery time beyond that sum.
Missing values in existing requests and persisted policies mean zero. Zero is
the default for configured and legacy consumers and preserves current behavior.
`configure_consumer` is a complete policy replacement: omitting
`retry_delay_ms` resets it to zero rather than preserving a previously
configured delay. Mixed-version policy writes are therefore unsupported while
any consumer relies on a nonzero delay.

For each assigned attempt, the acknowledgement lease remains exactly
`ack_timeout_ms`. An acknowledgement received at or after the lease deadline
is stale, even while a retry is waiting. Lease expiry remains demand-driven.
When a committed poll or stale-acknowledgement operation first observes that
the lease has expired, it durably schedules the next attempt at:

```text
retry_not_before = first_durable_expiry_observation + pinned_retry_delay
```

A zero delay makes the deadline equal the observation time and preserves
immediate retry eligibility under the existing poll lifecycle.
The delay starts when expiry is first durably observed, not at the lease
deadline. A late poll or stale acknowledgement therefore starts a fresh full
delay from that observation; a poll after `retry_not_before` can assign the
next attempt immediately. If no operation observes expiry, the delay has not
started. The delay is applied only before another delivery attempt. If the
inclusive attempt limit has been reached, the existing terminal/dead-letter
transition is eligible at the first durable expiry observation and does not
wait through an unused retry delay.
The terminal action keeps the existing engine guarantees: local movement
appends the dead-letter record before source progress and reconciles by its
internal move identity, while clustered movement is atomic only when source
and derived target share the same data group. The retry delay adds no
provenance and does not strengthen those at-least-once boundaries.

Attempt counts still advance only when a new assignment is durably committed.
The first-assignment policy snapshot includes `retry_delay_ms`, and every
retry for that offset uses the pinned value even after consumer configuration
changes. Changing the configured delay advances the policy version and
applies to records without a pinned attempt snapshot. There is no explicit
negative-acknowledgement or per-message retry operation in this decision.

Both engines use the first durable expiry observation as the delay start:

- **Local:** active leases continue to use their process-local monotonic
  deadline. The first local poll or stale acknowledgement that observes
  expiry persists `retry_not_before = local_wall_time_at_observation +
  retry_delay_ms` as part of its durable transition. A zero delay makes a
  poll eligible for immediate reassignment under the existing lifecycle. The
  schedule remains visible to candidate selection, including its ordering-key
  gate, and survives later restarts. If restart discards the active lease
  before any operation durably observed expiry, the first post-restart
  operation that observes expiry starts and persists the full delay. Local
  retry deadlines are absolute wall-clock timestamps evaluated on that clock
  both before and after restart, so a clock jump can make them early or late.
- **Clustered:** the replicated lease deadline and pinned policy survive
  process restart, snapshot recovery, and leader transfer. The first committed
  operation that observes expiry persists
  `retry_not_before = effective_observation_time + retry_delay_ms` in
  replicated per-offset delivery state. A zero delay makes a poll eligible
  for immediate reassignment under the existing lifecycle. The existing effective clock,
  `max(persisted_lease_clock, command_observation)`, determines the observation
  time and whether the deadline is due. No periodic timer is introduced; a
  committed poll or stale-acknowledgement command makes progress. A node
  restart or leadership change does not reset a deadline already persisted.

This rule makes late observation and restart consistent: neither engine
backdates the delay to the lease deadline. A local or clustered restart that
occurs before expiry was durably observed leaves the attempt unscheduled; the
first later operation that observes expiry starts the same full delay. If a
schedule was already persisted, both engines preserve it across restart or
leadership change. Because expiry observation is demand-driven, a late poll
can extend total time beyond `ack_timeout_ms + retry_delay_ms`; without a
command there is no expiry observation or retry progress.

While a retry is delayed, the source offset remains pending and cannot be
acknowledged with its expired token. It continues to reserve its requested
ordering key so a later record with that same key cannot overtake it. Other
eligible work follows the engines' existing candidate-ordering rules. Delivery
remains at least once: a consumer may have completed an external effect before
its acknowledgement was lost, and the broker can redeliver that work.

The fixed delay does not grow with attempt number and has no jitter. Each
delay is capped by the seven-day field bound; an unlimited attempt policy can
still retry indefinitely. Delayed work remains demand-driven and must not
require one runtime task per record.

## Rationale and alternatives

The fixed delay gives applications a direct control over the gap after the
broker observes an expired attempt and before its next delivery without
lengthening the healthy-work acknowledgement lease. Apache Pulsar documents acknowledgement
timeout backoff separately from its lease and shows the retry interval as the
acknowledgement timeout plus a backoff value. NATS JetStream instead documents
its `BackOff` sequence as replacing `AckWait`, with the first entry defining
the initial acknowledgement deadline. Runnel chooses the additive model so
the existing lease contract stays stable. Both systems keep timeout and
negative-acknowledgement behavior distinct, supporting the decision not to
invent a negative-acknowledgement surface here.

Exponential growth and jitter can reduce repeated retry pressure and
synchronized bursts, as AWS's published contention analysis demonstrates.
They are deferred because they add attempt-dependent arithmetic or a
deterministic distribution contract before Runnel has measured a
representative retry workload. A fixed delay is an intentionally small first
control; its synchronized-retry risk remains explicit. The implementation
must preserve bounded scheduler state and benchmark material polling or
persistence hot-path changes.

Using the lease timeout itself as the retry delay was rejected because it
would force applications to choose between healthy processing time and
failed-work spacing. Resetting an already persisted clustered deadline on
leader change was rejected because it would make scheduled state depend on
ownership timing. Persisting the local active lease deadline in a shared
wall-clock model solely to backdate retry eligibility to the
lease-deadline-plus-delay timestamp was also rejected: it would replace the
local engine's existing volatile monotonic lease boundary. Starting the delay
when expiry is first durably observed keeps that lease boundary and gives late
observations and recovery one rule across both engines.

## Consequences and implementation gates

- Existing configurations and persisted policies read with
  `retry_delay_ms = 0`; no default timing changes.
- Policy serialization and protocol fixtures must cover an absent field,
  zero, the seven-day maximum, rejection above the maximum, and policy-version
  pinning across a later configuration change.
- Local tests must cover the exact lease-expiry observation and
  `retry_not_before` boundaries, late poll and stale-ack observation, same-key
  exclusion, unrelated work, restart before expiry is durably observed,
  restart after a schedule is recorded, and restart after its deadline. A
  restart before durable expiry observation must start one full delay on the
  first post-restart operation; a recorded deadline must not be reset.
- Clustered tests must cover the same observation and deadline boundaries,
  late poll and stale-ack observation after restart or leader transfer,
  persistence through state-machine replay and snapshot recovery, leader
  transfer without resetting a recorded deadline, same-key exclusion,
  stale-token fencing, and terminal handling at expiry without an unused
  delay.
- A real multi-process test must establish the externally visible clustered
  behavior. The state-machine clock should be controllable in deterministic
  tests; process-level tests must not claim hard real-time precision.
- Rolling upgrades or rollback across binaries that do not know this policy
  field are not covered by the provisional protocol. Runtime work must either
  reject unsafe mixed-version policy use or establish a compatibility gate;
  silently dropping a nonzero field is not acceptable.

Clustered not-before deadlines inherit TD-020: a forward wall-clock jump may
make the retry due early, a backward jump can delay it until the persisted
floor catches up, and no quorum or no subsequent command means no progress.
The deadline is a scheduling contract under those assumptions, not a hard
real-time guarantee. Local active leases use a monotonic deadline
in-process, but a persisted local retry deadline uses wall time both before
and after restart and can be affected by clock jumps; unlike the cluster,
local retry state currently has no persisted clock floor. A restart before expiry is durably
observed starts the delay at the first operation that observes it, because no
retry schedule exists yet. Rust's
`std::time::Instant` documentation also leaves suspend accounting
platform-dependent; the live local interval is defined on that existing
monotonic clock, not as a guaranteed wall-clock duration while the host is
suspended.

## References

- Apache Pulsar, [consumer acknowledgement-timeout redelivery backoff](https://pulsar.apache.org/docs/client-libraries/consumers/#acknowledgment-timeout-redelivery-backoff) and [ConsumerBuilder API](https://pulsar.apache.org/api/client/4.0.x/org/apache/pulsar/client/api/ConsumerBuilder.html).
- NATS JetStream, [acknowledgment and redelivery behavior](https://docs.nats.io/learn/jetstream/acknowledgment).
- Marc Brooker, AWS Architecture Blog, [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
- Rust standard library, [`std::time::Instant`](https://doc.rust-lang.org/std/time/struct.Instant.html).
- TD-020, [clustered delivery leases use absolute wall-clock deadlines](../tech-debt.md#td-020-clustered-delivery-leases-use-absolute-wall-clock-deadlines).
