# TD-022: Local durable-I/O isolation evidence

- Status: exploratory evidence note; no executor or concurrency change authorized
- Last reviewed: 2026-09-06
- Baseline: `a667889195fe566b636b084649660f5ffdd09ad8`
- Scope: local synchronous filesystem work, asynchronous admission, stream ordering, and slow-I/O behavior
- Related debt: [TD-022](../tech-debt.md#td-022-local-durable-io-has-bounded-async-isolation-but-incomplete-evidence)
- Related outcome: [Make concurrent broker work scale predictably](../backlog.md#make-concurrent-broker-work-scale-predictably)
- Related policy: [Durability and delivery policy boundary](durability-delivery-policy.md)

This note records what the local async boundary currently guarantees, what it
costs, and which evidence is still needed before changing executor sizing,
fairness, durability batching, or broker concurrency. It does not select an
executor design, add configuration, or authorize a runtime change. Rust code
and tests remain authoritative if this note becomes stale.

## Question

Can a local filesystem stall remain bounded to the affected operation while
unrelated broker work continues, without allowing the async runtime, storage
memory, or per-stream ordering guarantees to be consumed by unbounded waiting
work?

The current answer is deliberately narrower than “local storage is
non-blocking.” Async network requests are kept off the runtime thread and
admitted blocking work is bounded, but a filesystem call that has started is
still synchronous and cannot be canceled by the caller's request timeout.

## Observed baseline

### Async boundary and ordering

The local `Engine` adapter sends create, publish, batch publish, poll, replay,
grouped poll, acknowledgement, and health operations through one
`StorageExecutor` per `BrokerState` ([adapter](../../crates/runnel-core/src/lib.rs#L72)).
The synchronous `Broker` methods remain available and are intentionally not
wrapped by this boundary; callers that invoke those methods directly are
responsible for keeping blocking work off their own async runtime.

Broker startup is also outside this boundary: `Broker::open` creates the
directory structure and scans existing stream logs synchronously before the
engine is available. The executor therefore isolates steady-state async
requests, not startup recovery or every filesystem operation in the process.

The executor has the following fixed defaults:

| Boundary | Current behavior | Consequence |
| --- | --- | --- |
| Execution | 32 permits; each admitted operation runs in `spawn_blocking` after acquiring one. | At most 32 Runnel storage closures execute concurrently for one local broker. Other uses of Tokio's blocking pool are outside this accounting. |
| Unstarted operation admission | 64 permits: 32 execution slots plus 32 operations waiting for one. | A new unlaned operation receives `WouldBlock` once the global bound is full; it does not wait unboundedly in the executor. |
| Per-stream lane | One active owner, FIFO waiters, and a 32-entry lane queue. | Same-stream async operations preserve submission order before touching the stream state lock. |
| Cross-stream lane waiting | 32 permits shared by all stream waiters. | A busy stream cannot create unbounded async tasks waiting on lanes, but many busy streams compete for the same waiter budget. |
| Lane registry | Stream names map to weak lane references; active and queued work holds strong references. | One-off stream names do not permanently retain lane state, while in-flight work remains safe. |

The lane is an execution and backpressure boundary, not a semantic ordering
model. The stream mutex still protects the `StreamState`, and storage calls
under that mutex include record reads, appends, consumer-state persistence,
and recovery-related state changes. A different stream can run concurrently if
an execution permit is available; operations on the same stream cannot bypass
the lane to gain parallelism ([executor](../../crates/runnel-core/src/storage.rs#L15)).
Health is dispatched without a stream lane and then locks and measures every
stream in sequence ([health](../../crates/runnel-core/src/broker.rs#L410)).

### Durable work and caller-visible outcomes

The executor controls where synchronous work runs, not what a successful
operation means. The current local durability boundaries are:

| Operation | Durable work before success | Relevant limit |
| --- | --- | --- |
| Single publish or request-aware append | Writes a complete frame and calls `sync_data`. | One stream lock and one synchronous sync per append. |
| Publish batch | Appends records, then calls one `sync_data` when at least one record was appended. | The batch is bounded by the protocol/core record limit, but its encoded byte size and sync duration are workload-dependent. |
| Poll or grouped poll | Persists a delivery-attempt event with `sync_all` before returning the message. | A slow consumer-state filesystem operation can hold the stream lane. |
| Acknowledgement | Persists an acknowledgement event with `sync_all` before advancing in-memory progress. | Per-event durability remains synchronous; it is not amortized by the executor. |
| Health | Reads stream metadata and delivery counts while holding each stream lock. | Work grows with stream count and can wait behind a stream operation. |

The stream-log boundaries are visible in [stream log append](../../crates/runnel-core/src/stream_log.rs#L164)
and the consumer journal in [consumer-state persistence](../../crates/runnel-core/src/consumer_state.rs#L91).
Changing the executor does not make a publish, delivery attempt, or
acknowledgement durable earlier, and changing a sync primitive or batching
policy would be a separate durability decision.

Admission failure is represented internally as `BrokerError::Io` with
`ErrorKind::WouldBlock`. The provisional server maps that family to the
generic `storage_error`; a request that waits until its protocol deadline
instead receives `request_timeout`. There is currently no public distinction
between executor saturation, disk pressure, a failed sync, and a write whose
response was lost after the filesystem may have committed it. That ambiguity
is a policy/contract gap, not a reason to silently retry a timed-out write.

## What current tests establish

The focused `runnel-core` tests cover the executor's basic safety and bound
properties:

- storage closures run away from a current-thread runtime;
- unrelated async work runs while one storage closure is stalled;
- execution waits when the active slot is occupied;
- global admission rejects work beyond the configured bound;
- canceling a queued operation drops its closure before execution;
- same-stream operations remain FIFO while another stream progresses;
- aggregate stream waiters are bounded across lanes; and
- canceling a lane waiter releases its queue capacity.

These are unit-level scheduler tests using controlled closures, not filesystem
performance measurements. They are implemented in
[`storage.rs`](../../crates/runnel-core/src/storage.rs#L350).

The core async adapter also verifies that a held stream lock does not block
unrelated work on a current-thread runtime and that concurrent publishes retain
offset order, restart redelivery, and acknowledgement recovery
([core async tests](../../crates/runnel-core/src/lib.rs#L312)).

The real-process admission tests add an important boundary. A FIFO placed at a
consumer journal path stalls a real server operation and currently verifies
that:

- the stalled request reaches its 500 ms protocol timeout in under one second;
- readiness and metrics remain bounded and metrics stay scrapeable while the
  engine health dependency is unavailable;
- an independent stream can publish and poll while the stalled stream is
  blocked;
- a same-stream waiter canceled by its request timeout does not poison the
  following request; and
- after the FIFO is released, the server can shut down, restart, and recover
  durable state.

See [`admission.rs`](../../crates/runnel-server/tests/admission.rs#L673) for
the traffic/readiness case, the waiter-recovery case, and the shutdown/restart
case. The existing technical-debt entry records five serial repetitions of
the two FIFO-stall cases on 2026-09-04, with the stated request, readiness,
metrics, shutdown, and recovery bounds.

## What current tests do not establish

The FIFO is a useful end-to-end blocking-point fixture, but it is not a slow
or failing production filesystem. In particular, the repository does not yet
show:

- per-request or per-stage p50/p99/p99.9 latency under controlled slow-I/O
  delay, including queue wait, lane wait, lock wait, read/write, and sync;
- CPU, resident-memory, and blocking-pool occupancy under global saturation;
- behavior across one busy stream, many busy streams, and a mix of hot and
  cold streams when all 32 execution permits are occupied;
- a separately measured executor queue or lane saturation signal in metrics;
- the behavior of a filesystem call that has entered `write`, `sync_data`, or
  `sync_all` and cannot be interrupted; or
- a process-kill/restart test at an actual local stream or consumer-state
  write/sync boundary.

`tokio::time::timeout` cancels the async future while it is waiting for a
permit or for the blocking task to be joined. Once `spawn_blocking` has
started its closure, the synchronous call may continue after the request
returns a timeout. The server's bounded shutdown tests release the FIFO before
waiting for process exit; they therefore demonstrate recovery from a released
stall, not a universal bound for an uninterruptible operating-system call.
The lifecycle drain has a 25-second bound, but aborting the outer async task
does not make an already-running blocking closure interruptible
([connection execution](../../crates/runnel-server/src/connection.rs#L245),
[lifecycle drain](../../crates/runnel-server/src/lifecycle.rs#L62)).

The existing Criterion suite is useful for local regression detection. It
measures durable publish, publish/poll/ack, shared-consumer, keyed shared
consumer, and concurrent publish cases, generally with 100-byte payloads and
temporary directories ([broker benchmarks](../../crates/runnel-core/benches/broker.rs#L85)).
Those cases do not inject a slow filesystem or expose executor queue and lane
wait times, so they cannot retire TD-022 by themselves.

## Contention and cost limits

The current design has several deliberate costs that should remain visible in
future comparisons:

1. A single stream is serialized even when operations are logically
   independent. This is necessary for the current stream-state lock and local
   offset/order model, but it makes same-stream throughput and tail latency
   sensitive to the slowest operation in that stream.
2. A busy stream can consume one execution slot and hold queued same-stream
   callers behind it. The explicit lane limits prevent those callers from
   consuming all global execution slots, but a hot stream still receives only
   one active operation at a time.
3. Unrelated streams share one fixed executor and one aggregate lane-waiter
   budget. Fairness between streams is bounded by the current FIFO/permit
   interactions, not by a measured service-level guarantee.
4. Health is bounded at the server boundary but has no reserved executor
   capacity. Under enough admitted storage work, health may wait for a global
   execution permit or a stream lock until its health deadline.
5. The capacities are compile-time defaults rather than deployment policy.
   There is no evidence that 32 active and 32 queued operations fit every
   supported CPU, memory, filesystem, or request-admission configuration.
6. The executor does not amortize filesystem syncs. Local consumer delivery
   bookkeeping still pays the synchronous journal boundary described in
   [TD-019](td-019-delivery-bookkeeping.md), and stream appends still pay their
   own durable sync.

These limits are reasons to measure before changing the scheduler, not proof
that a different executor would improve the product. A larger queue can trade
rejection for memory and tail latency; more execution slots can trade local
parallelism for filesystem contention; and a dedicated health path can trade
isolation for capacity reserved from message work.

## Concrete future options, not requirements

Future work can evaluate the following approaches against the current
behavior without committing to module names, public APIs, or a storage layout:

- retain the current bounds and add admission, lane-wait, lock-wait, and
  storage-stage evidence first;
- reserve a small control/health capacity so liveness and operator signals do
  not compete with all durable traffic;
- use a fair bounded scheduler that accounts for stream identity and operation
  class while preserving FIFO within each required ordering domain; or
- reduce synchronous durability cost through bounded batching or group commit,
  but only under the separate durable-outcome and recovery gates in TD-019.

Each option has a different caller-visible failure and timeout model. None
should be treated as an implementation prescription until a comparison
defines what remains ordered, what is durable before success, how cancellation
works, and what a response-loss retry can observe.

## Outcome and evidence gates

Retire or materially revise TD-022 only when a candidate or the retained
baseline has evidence for all of the following:

### Isolation and bounds

- A real-process test stalls one local durable operation and verifies progress
  for unrelated streams, health, readiness, metrics, and shutdown under an
  explicit request and process deadline.
- Tests cover global executor saturation, one hot stream versus many hot
  streams, lane queue saturation, cancellation of queued work, and recovery of
  the next operation after a timeout.
- The evidence reports active work, queued work, lane waiters, memory, and
  resource limits; no unbounded task or lane growth is inferred from a unit
  test alone.

### Durable outcomes and recovery

- A real-process or deterministic fault test covers partial/failed writes,
  failed syncs, response loss, process interruption, and restart for publish,
  delivery-attempt persistence, acknowledgement, and any future batching.
- A timeout is classified as “not started,” “still running,” “failed,” or
  “possibly committed” where the implementation can distinguish those states;
  the client is not instructed to blindly retry an ambiguous durable write.
- Existing at-least-once delivery, acknowledgement ordering, stale-token
  fencing, incomplete-tail recovery, and corruption handling remain unchanged.

### Performance and operations

- A resource-scoped comparison names the exact baseline revision, filesystem,
  CPU and memory limits, payload/key mix, stream count, worker count, operation
  mix, durability mode, and repetition/stability policy.
- It reports throughput plus p50, p99, and p99.9 latency where meaningful,
  separately for queue/lane/lock/I/O work when instrumentation supports that
  split. Results must retain observed ranges and call noisy or mismatched runs
  inconclusive.
- The comparison includes one, two, four, and eight workers; same-stream and
  independent-stream workloads; publish, poll/ack, grouped delivery, batch,
  health, and slow-I/O cases as applicable. It must report memory, CPU,
  storage bytes, and rejection/timeout behavior alongside throughput.
- Any new capacity or priority policy is validated at startup, documented as a
  bounded operational setting, exposed through useful metrics, and covered by
  overload and shutdown tests.

If these gates are not met, retain the current bounded executor and keep
TD-022 open. If a runtime or public outcome changes, record the accepted
durability, cancellation, timeout, and compatibility consequences in an ADR
before treating it as supported behavior.

## Refactor and planning assessment

No safe runtime refactor is included. The local storage executor, stream lane,
stream state, and server admission boundaries are already separated enough to
measure the current behavior. A broader scheduler or durability refactor would
couple ordering, filesystem, request-outcome, and shutdown semantics before
the missing evidence exists. The existing TD-022 entry covers that future
work; no additional tech-debt item is needed.
