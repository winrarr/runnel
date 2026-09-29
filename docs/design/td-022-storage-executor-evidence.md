# TD-022: Local durable-I/O isolation evidence

- Status: exploratory evidence note; no executor or concurrency change authorized
- Last reviewed: 2026-09-29
- Baseline: `62793482c849940cfc1f0d99afe960e014d37da4`
- Baseline CI state supplied with this assignment: run [#36592168031](https://github.com/winrarr/runnel/actions/runs/36592168031) was pending
- Scope: local synchronous filesystem work, asynchronous admission, stream ordering, and slow-I/O behavior
- Related debt: [TD-022](../tech-debt.md#td-022-local-durable-io-has-bounded-async-isolation-but-incomplete-evidence)
- Related outcome: [Make concurrent broker work scale predictably](../backlog.md#make-concurrent-broker-work-scale-predictably)
- Related policy: [Durability and delivery policy boundary](durability-delivery-policy.md)
- Related research: [Systems performance research for Runnel](../research/systems-performance-research.md)

This note records what the local async boundary currently guarantees, what it
costs, and which evidence is still needed before changing executor sizing,
fairness, durability batching, or broker concurrency. It does not select an
executor design, add configuration, claim a performance improvement, or
authorize a runtime change. Rust code and tests remain authoritative if this
note becomes stale.

## Question

Can a local filesystem stall remain bounded to the affected operation while
unrelated broker work continues, without allowing the async runtime, storage
memory, or per-stream ordering guarantees to be consumed by unbounded waiting
work?

The current answer is narrower than “local storage is non-blocking.” Async
network requests are kept off the runtime thread and admitted blocking work is
bounded. A filesystem call that has started remains synchronous and cannot be
canceled by the caller's request timeout.

## Observed baseline

### Async boundary, startup, and ordering

The local `Engine` adapter sends stream creation, publish and publish batch,
poll and grouped poll, consumer-policy configure and inspect, replay,
acknowledgement and grouped acknowledgement, and health through one
`StorageExecutor` per `BrokerState` ([adapter](../../crates/runnel-core/src/lib.rs#L73),
[executor](../../crates/runnel-core/src/storage.rs#L12)). The synchronous
`Broker` methods remain available and are intentionally not wrapped by this
boundary; callers that invoke them directly are responsible for keeping
blocking work off their own async runtime.

Broker startup is outside the executor. `Broker::open` creates the storage
directories and synchronously scans and opens existing stream logs before
constructing the executor ([startup](../../crates/runnel-core/src/broker.rs#L74)).
The executor therefore isolates steady-state async engine requests, not
startup recovery or every filesystem operation in the process.

The executor uses fixed capacities ([defaults and dispatch](../../crates/runnel-core/src/storage.rs#L12)):

| Boundary | Current behavior | Consequence |
| --- | --- | --- |
| Execution | 32 permits; each admitted operation runs in `spawn_blocking` after acquiring one. | At most 32 Runnel storage closures execute concurrently for one local broker. Other uses of Tokio's blocking pool are outside this accounting. |
| Global operation admission | 64 permits: 32 execution slots plus 32 operations admitted while waiting for one. Admission is non-waiting and returns `WouldBlock` at the bound. | Active and execution-queued storage closures are bounded. A request that waits on a stream lane is outside this count. |
| Per-stream lane | One active owner and FIFO waiters; at most 32 waiters in a lane. | Same-stream async operations enter the broker in submission order before taking the stream-state lock. |
| Cross-stream lane waiters | 32 permits shared by all stream-lane waiters. | A hot stream cannot create unbounded waiters, but waiters for different busy streams share this limit. |
| Lane registry | Stream names map to weak lane references; active and queued work holds strong references. | One-off stream names do not permanently retain lane state, while in-flight work remains safe. |

Stream waiters acquire a lane before global operation admission, so they do not
occupy one of the 64 admitted storage operations while waiting for their
stream's owner. Once a waiter reaches the head, it attempts global admission;
if that bound is full, the request is rejected and releases the lane. The lane
is an execution and backpressure boundary, not the semantic ordering model.
The stream mutex still protects `StreamState`; filesystem calls under it
include record reads, appends, consumer-state persistence, and recovery-related
state changes. A different stream can run concurrently when an execution
permit is available, while operations in one stream cannot bypass its lane
([lane implementation](../../crates/runnel-core/src/storage.rs#L214)).

Health dispatch has no stream lane, but it uses the same global executor and
then locks and measures every stream in sequence ([health adapter](../../crates/runnel-core/src/lib.rs#L202),
[snapshot](../../crates/runnel-core/src/broker.rs#L497)). Therefore it can fail
at global admission or wait on a stream mutex held by a blocked storage call;
the two cases are exercised separately by the real-process tests below. The
HTTP readiness and metrics handlers bound how long they await health to one
second ([bounded health](../../crates/runnel-server/src/observability.rs#L297)),
but dropping that await does not stop a health closure already running
in `spawn_blocking`. Such a closure can remain blocked on the stream mutex and
retain its executor permits until the storage call releases the lock. Endpoint
response bounds therefore do not establish that repeated probes preserve
capacity under a prolonged lock stall.

### Durable work and caller-visible outcomes

The executor controls where synchronous work runs, not what a successful
operation means. The local durability boundaries remain:

| Operation | Durable work before success | Relevant limit |
| --- | --- | --- |
| Single publish or request-aware append | Writes a complete frame and calls `sync_data`. | One stream lock and one synchronous sync per append. |
| Publish batch | Appends records, then calls one `sync_data` when at least one record was appended. | Ordered per-record outcomes do not imply transaction atomicity; encoded bytes and sync duration depend on the workload. |
| Consumer-policy configure | Persists a policy event with `sync_all` before updating cached state. | Consumer configuration shares the stream lane and consumer-journal durability boundary. |
| Poll or grouped poll | Persists a delivery-attempt event with `sync_all` before returning the message. | A slow consumer-state filesystem operation holds the stream lane and mutex. |
| Acknowledgement or grouped acknowledgement | Persists an acknowledgement event with `sync_all` before advancing in-memory progress. | Per-event durability remains synchronous; it is not amortized by the executor. |
| Consumer inspect or replay | Loads consumer state or reads retained stream data while holding the stream mutex. | Read and recovery work also occupies a lane and execution slot; replay does not mutate consumer progress. |
| Health | Reads stream metadata and delivery counts while holding each stream lock. | Work grows with stream count and may wait behind a stream operation. |

See [stream-log append](../../crates/runnel-core/src/stream_log.rs#L182)
and [consumer-state persistence](../../crates/runnel-core/src/consumer_state.rs#L144).
Changing the executor does not make a publish, delivery attempt, or
acknowledgement durable earlier. Changing a sync primitive or batching policy
would be a separate durability decision.

Per-stream or aggregate lane-waiter exhaustion returns `BrokerError::Io` with
`ErrorKind::WouldBlock` and the diagnostic “storage stream queue is full”;
global storage-admission exhaustion returns the same error kind with “storage
execution queue is full”. The provisional server maps either to the generic
`storage_error` code
([dispatch mapping](../../crates/runnel-server/src/dispatch.rs#L318)). If a
request instead waits until its protocol deadline, the server returns
`request_timeout` ([request execution](../../crates/runnel-server/src/connection.rs#L245)).
Cancellation while queued on a stream lane removes that waiter; cancellation
while waiting for execution prevents a closure from starting. Once
`spawn_blocking` has started the closure, dropping the async request or its
join handle does not stop the synchronous filesystem call. It may finish after
the client received a timeout, leaving the durable outcome unknown to that
client. [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)
classifies generic storage failures conservatively as `Unknown`; the
provisional protocol still does not distinguish executor saturation, device
pressure, sync failure, and a possibly committed write whose response was
lost. These distinctions do not justify silently retrying a timed-out write.

Optional instrumentation adds a `core.storage_dispatch` timer and separate
timers for stream-lock wait, append/read, and consumer-state persistence
([timer sites](../../crates/runnel-core/src/storage.rs#L117)). These timers
are absent from the normal build. `core.storage_dispatch` covers the overall
async dispatch path; it does not separately report lane wait, global
admission wait, execution-permit wait, and closure service time. The documented
instrumented profile is a clustered workload, so it is not evidence for this
local executor ([profile workflow](../../scripts/benchmarks/README.md#profiling)).
Normal server metrics expose request-level activity, failures, and timeouts,
but no live storage-executor occupancy, queue-depth, or lane-waiter gauges.

### What current tests establish

The focused `runnel-core` executor tests use controlled closures rather than
filesystem performance workloads ([tests](../../crates/runnel-core/src/storage.rs#L344)).
They establish that:

- storage closures run away from a current-thread runtime and unrelated async
  work can proceed while one closure is held;
- execution waits within the configured bound and global admission rejects
  beyond it;
- canceling a globally queued operation prevents its closure from running;
- a same-stream lane preserves FIFO order while a different stream progresses;
- aggregate stream waiters are bounded across lanes; and
- canceling a lane waiter releases its queue capacity.

The core async adapter also holds a stream mutex to model the blocking point,
then checks that unrelated work on a current-thread runtime remains responsive.
Separate adapter tests cover concurrent-publish offset order and restart
recovery ([core async tests](../../crates/runnel-core/src/lib.rs#L342)). The
held-mutex test is not an injected slow filesystem measurement.

The real-process server tests use FIFOs at the consumer-state journal path
(`consumer.json.tmp`, [path construction](../../crates/runnel-core/src/consumer_state.rs#L221))
to create deterministic blocking points in actual filesystem calls. The
global-saturation probe blocks journal reads; the same-stream pressure path
also reaches an append to the FIFO after releasing its held read. The tests
exercise network requests, server timeouts, health endpoints, metrics, release,
and recovery, but not a slow or full storage device:

- [`storage_stall_is_bounded_and_durable_traffic_continues`](../../crates/runnel-server/tests/admission.rs#L1337)
  uses a 500 ms request deadline. It expects the stalled poll and protocol
  health request to time out within one second, readiness and metrics to
  respond within two seconds, and a different stream to publish and poll
  durably within one second while the FIFO remains blocked. The metrics
  endpoint returns HTTP 200 with `runnel_engine_health_available 0`, retains
  process/admission samples, and omits engine-derived samples until recovery
  ([readiness handler](../../crates/runnel-server/src/observability.rs#L305),
  [metrics fallback](../../crates/runnel-server/src/observability.rs#L344)).
- [`timed_out_same_stream_waiter_does_not_poison_following_request`](../../crates/runnel-server/tests/admission.rs#L1485)
  holds one poll in the FIFO, lets a second same-stream request hit its 1.5 s
  timeout, releases the FIFO, and verifies the next poll and health request
  succeed. It demonstrates waiter cleanup, not cancellation of the blocked
  filesystem call.
- [`storage_stall_shutdown_is_bounded_and_restart_recovers`](../../crates/runnel-server/tests/admission.rs#L1584)
  sends SIGTERM while the operation is blocked, then releases the FIFO before
  waiting for process exit. The released-stall shutdown completes within two
  seconds and restart recovers the prior durable publish. It does not measure
  exit while a call remains blocked.
- [`sustained_storage_pressure_is_bounded_observable_and_recovers`](../../crates/runnel-server/tests/admission.rs#L623)
  holds one storage operation on a stream, queues 32 same-stream waiters, and
  checks prompt rejection of additional polls as `storage_error`, without
  counting them as request timeouts. It checks request-failure metrics,
  liveness and scrapeable fallback metrics, unrelated durable publish, and
  queued-poll completion and health recovery after FIFO release. Health
  dispatch bypasses the stream lane but blocks on the held stream mutex until
  its one-second health deadline.
- [`global_storage_admission_is_bounded_observable_and_recovers`](../../crates/runnel-server/tests/admission.rs#L928)
  holds 32 distinct-stream reads in FIFOs and sends 64 candidate polls on
  distinct streams. It observes 32 prompt global-admission `storage_error`
  responses while 32 candidates remain admitted behind 32 running operations.
  An extra poll and protocol health request are rejected promptly at global
  admission. Readiness reports 503, liveness and the HTTP 200 metrics fallback
  remain available within one second, and unavailable engine samples are
  omitted. Releasing the FIFOs lets all admitted polls finish and restores
  health, readiness, metrics, and durable traffic.

The tech-debt record also preserves previously measured repeated evidence: on
2026-09-04, `storage_stall_is_bounded_and_durable_traffic_continues` and
`storage_stall_shutdown_is_bounded_and_restart_recovers` passed five serial
repetitions, ten test cases total, with a 500 ms request timeout, the bounds
listed in that record, and 5.67–5.76 seconds per paired test run after
compilation ([TD-022 run record](../tech-debt.md#td-022-local-durable-io-has-bounded-async-isolation-but-incomplete-evidence)).
That prior measurement is not a new run at this baseline. No tests or runtime
commands were run for this documentation refresh.

### What current tests and measurements do not establish

The FIFO tests block a real server-side filesystem call at a controlled path,
but releasing a FIFO is not a model of a slow or full device. Existing
coverage does not establish:

- behavior or recovery under representative device latency, `ENOSPC`, or a
  real OS-level `sync_data`/`sync_all` failure. A focused test-only injected
  consumer-event sync failure exists for dead-letter recovery
  ([core test](../../crates/runnel-core/src/lib.rs#L1394)), but it does not
  simulate a device failure or a process crash during that call;
- shutdown or force-exit bounds while a started filesystem call remains
  uninterruptible; current shutdown evidence releases the FIFO first;
- fairness or service-time guarantees between busy streams, including a
  measured one-hot-stream versus many-hot-stream and hot/cold mix at global
  saturation; the global FIFO probe verifies the admission boundary, not
  which admitted stream receives service first or its latency;
- local slow-I/O latency distributions split into lane, admission, execution,
  lock, and filesystem service times, or CPU, resident-memory, and
  blocking-pool occupancy under a fixed resource budget. Optional stage timing
  can summarize some emitted samples with p50, p99, and maximum, but the
  current timing sites do not separate each executor wait component and no
  p99.9 local slow-I/O result is recorded. There is also no measurement of
  executor capacity consumed by repeated health closures left waiting on a
  stream mutex after their HTTP health deadline expires;
- a process-kill/restart probe at an actual local stream or consumer-state
  write/sync boundary. Existing ordinary restart, journal replay, incomplete
  tail, and corruption tests cover other recovery contracts, not interruption
  at that in-flight boundary; or
- startup recovery time or resource use while synchronously opening a large
  set of stream logs.

The normal `runnel-core` Criterion suite includes durable single and batch
publish, async-engine publish, publish/poll/ack, shared-consumer, retained
history, and concurrent publish cases ([benchmark definitions](../../crates/runnel-core/benches/broker.rs#L88)).
Most cases call synchronous `Broker` methods and bypass `StorageExecutor`;
`async_engine_publish` exercises the adapter but publishes sequentially, so it
does not measure executor queue contention or slow-I/O isolation. These
benchmarks remain regression evidence for their workloads, not TD-022
capacity, fairness, or device-stall evidence. Instrumented profiling can
report internal timing samples, but it is opt-in and does not by itself
provide the missing controlled slow-I/O and resource-pressure comparison
([benchmarking policy](../benchmarking.md)).

## Contention and cost limits

The current design has several deliberate costs that should remain visible in
future comparisons:

1. A single stream is serialized even when operations are logically
   independent. This makes same-stream throughput and tail latency sensitive
   to the slowest operation in that stream.
2. One stalled operation owns an execution permit and holds its stream's lane
   and state mutex. Same-stream callers wait behind it up to explicit bounds;
   unrelated streams can use remaining executor capacity.
3. Unrelated streams share one fixed executor and one aggregate lane-waiter
   budget. Per-stream FIFO is established; there is no measured fairness or
   service-level guarantee between streams or operation classes.
4. Health has no reserved executor capacity. At global storage saturation it
   can be rejected before execution; with execution capacity available, it
   can still wait on a stream lock until the health deadline.
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

Future work can compare the retained bounded executor with alternatives after
the missing evidence identifies the bottleneck. Useful measurements should
separate, where instrumentation allows, protocol admission, stream-lane wait,
global executor admission, execution-permit wait, stream-lock wait, filesystem
read/write/sync, and response delivery. Include same-stream and many-stream
workloads, hot/cold mixes, slow consumers, and explicit device/resource
pressure. Keep any scheduler, reserved-capacity, or batching option
hypothetical until it states what remains ordered, what is durable before
success, how cancellation works, and what response-loss retry can observe.

## Outcome and evidence gates

The current FIFO tests establish useful bounded admission, timeout, fallback,
and recovery behavior. Retire or materially revise TD-022 only when a retained
baseline or candidate has evidence for the remaining isolation, durability,
fairness, resource, and operational questions:

### Isolation and bounds

- Any scheduler change retains real-process coverage for same-stream FIFO,
  global saturation, rejected excess work, cancellation of queued waiters,
  unrelated-stream progress, and recovery of the next operation after
  timeout. Existing synthetic FIFO tests cover these specific boundaries.
- Controlled slow-I/O or device-pressure evidence reports offered load,
  latency distribution, rejection/timeout behavior, active/queued storage
  work, memory, CPU, and explicit resource limits. The existing FIFO tests do
  not satisfy this measurement gate.
- A fairness claim is supported by measured service across one hot stream,
  many active streams, and a hot/cold mix; FIFO within one stream alone does
  not establish cross-stream fairness.
- Health, readiness, metrics, and shutdown behavior stay within explicit
  deadlines under the tested stall model. If the claim includes an
  uninterruptible call, evidence must keep that call blocked through shutdown
  or explain why such an OS-level test cannot be made reliable.

### Durable outcomes and recovery

- A real-process or deterministic fault test covers partial/failed writes,
  failed syncs, response loss, process interruption, and restart at the
  affected durable boundary for publish, delivery-attempt persistence,
  acknowledgement, and any future batching. This complements, rather than
  replaces, current journal replay and incomplete-tail recovery tests.
- A timeout is classified as “not started,” “still running,” “failed,” or
  “possibly committed” where the implementation can distinguish those states;
  clients are not instructed to blindly retry an ambiguous durable write.
- Existing at-least-once delivery, acknowledgement ordering, stale-token
  fencing, incomplete-tail recovery, and corruption handling remain intact.

### Performance and operations

- A resource-scoped comparison names the exact baseline revision, filesystem,
  CPU and memory limits, payload/key mix, stream count, worker count,
  operation mix, offered-load model, durability boundary, and repetition and
  stability policy. It reports throughput and p50/p99/p99.9 latency where
  meaningful, retaining observed ranges and marking noisy or mismatched runs
  inconclusive.
- Compare same-stream and independent-stream workloads, plus publish,
  poll/ack, grouped delivery, batch, health, and slow-I/O cases as applicable.
  Report memory, CPU, storage bytes, and rejection/timeout behavior alongside
  throughput. Split queue, lane, lock, and I/O time only where the instrumentation
  measures those stages directly.
- Any new capacity or priority policy is validated at startup, documented as
  a bounded operational setting, exposed through useful metrics, and covered
  by overload and shutdown tests.

If these gates are not met, retain the current bounded executor and keep
TD-022 open. Any runtime or public-outcome change still needs an accepted
durability, cancellation, timeout, and compatibility consequence in an ADR.
The server's 25-second task-drain timeout is not a 25-second guarantee for a
started blocking filesystem call: after that timeout the lifecycle aborts
async task handles, which does not interrupt an already-running blocking
closure ([shutdown drain](../../crates/runnel-server/src/lifecycle.rs#L73)).

## Refactor and planning assessment

No runtime refactor is proposed: this refresh only brings the evidence note
into line with the existing executor, tests, optional instrumentation, and
accepted error classification. Inspection found no immediate code refactor
that belongs in this evidence-only change. TD-022 and the concurrent-work
backlog item already record the near-term outcome and missing evidence, so no
additional or changed planning item is warranted. No ADR is changed because
the implementation and accepted decisions remain the same; [ADR 0001](../decisions/0001-single-node-durable-log.md)
and [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)
remain the relevant storage and outcome boundaries; [ADR 0020](../decisions/0020-stable-optimization-evidence.md)
governs any later optimization claim.
