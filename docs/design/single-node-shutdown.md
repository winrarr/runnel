# Single-node graceful shutdown

Status: exploratory proposal; no shutdown policy is accepted by this note.

Primary evidence class: reliability/operational safety. Secondary tags: process lifecycle, durability/recovery.

Last reviewed: 2026-09-29. Observed baseline: `9f4581da9f6e231164ef038578d13e010ef7a59f`.

## Purpose and scope

Define an outcome contract for stopping one local broker after SIGINT or SIGTERM. The proposal covers when the broker stops admitting client work, how already-dispatched requests and open connections are handled, what readiness and liveness mean while the process exits, how an expired drain deadline is reported, and what operators may infer after restart.

The first implementation slice should be the local engine only. It should not promise cluster quorum, leader transfer, peer-request draining, member departure, or availability while this broker is stopped. The early Raft backend has separate peer and replicated-state lifecycles; those need a distinct cluster design and evidence gate.

This proposal does not authorize a protocol change, a new administrative shutdown endpoint, changes to storage formats, or deployment-manifest edits. The exact graceful timeout, exit status on timeout, client-facing retry guidance, and configuration surface remain open choices for an ADR before implementation.

## Observed baseline

The backlog already requires repeatable graceful-shutdown and restart tests under “Make the single-node deployment ready for real use.” The overload backlog also requires that a slow or malformed connection not prevent shutdown. TD-022 already records the storage boundary that matters here: a started blocking filesystem call is not cancelled by the async request timeout or by aborting its caller, and its shutdown probe releases the FIFO before waiting for process exit. These records are the planning home; this note adds no backlog, tech-debt, or ADR entry.

At this baseline, the Unix signal listener handles SIGINT and SIGTERM. The lifecycle code first marks the process as shutting down, then broadcasts a watch signal to the TCP, HTTP, and optional peer listeners. Readiness switches to HTTP 503 immediately; liveness remains HTTP 200. The metrics route remains present while the HTTP task is draining. The server wraps the listener tasks in one fixed 25-second timeout.

The TCP accept loop stops accepting after the watch signal. Existing idle reads and partial frame reads observe the signal and close instead of waiting out their request timeout. A request that has already been parsed and dispatched does not select on shutdown: it can finish and send its response, subject to its request timeout and the outer 25-second drain. The TCP task joins its accepted client-connection tasks. Axum serves HTTP with its graceful-shutdown adapter, and that task is included in the same outer timeout.

In the local engine, a single durable publish appends its record and calls sync_data before returning its offset; consumer acknowledgement events are synced before the in-memory checkpoint advances. The response is written afterward by the server. The durable engine result and the client's observation of the response are therefore separate boundaries: connection loss after an operation commits does not undo it.

When the 25-second timeout expires, lifecycle aborts the TCP, HTTP, and peer listener tasks and still returns success. Aborting those async tasks is not a force-cancel of a started storage operation. The local engine runs synchronous storage operations with Tokio spawn_blocking. The pinned Tokio 1.53.1 documentation says started blocking tasks cannot be aborted and ordinary runtime drop waits for them to return. A blocked or uninterruptible filesystem call can therefore outlive the application-level 25-second drain even though the server logged a timeout. An external service manager's deadline may be the only final process bound in that case.

The peer listener also stops accepting on the shared signal, but accepted peer connections are spawned without a join set and are not included in the server's listener drain. This is outside the single-node proposal and is one reason the 25-second lifecycle should not be read as a cluster-wide shutdown guarantee.

There is a second terminal path worth keeping distinct from a signal: if a TCP or HTTP listener task returns an I/O error, lifecycle propagates that error before broadcasting the shutdown watch signal. Coordinated teardown is therefore not established for listener failure. A future implementation should preserve the original failure while still running bounded cleanup, and test that path separately from an operator-requested drain.

Existing real-process coverage verifies that SIGTERM closes an idle persistent client with a partial next frame promptly and that the process exits successfully. Other restart tests generally kill the process. The shutdown test does not prove the result of an in-flight publish or acknowledgement, deadline-expiry behavior, recovery after force termination at a write boundary, or peer-task drain. The storage-stall test described in TD-022 releases its FIFO before process exit, so it does not establish a hard exit bound for an uninterruptible write.

Observed implementation references: [lifecycle shutdown path](../../crates/runnel-server/src/lifecycle.rs#L47), [TCP accept and drain](../../crates/runnel-server/src/connection.rs#L52), [request-frame cancellation](../../crates/runnel-server/src/connection.rs#L132), [HTTP health handlers](../../crates/runnel-server/src/observability.rs#L293), [local blocking storage execution](../../crates/runnel-core/src/storage.rs#L152), [durable append](../../crates/runnel-core/src/stream_log.rs#L182), [acknowledgement persistence](../../crates/runnel-core/src/broker.rs#L480), [real-process shutdown test](../../crates/runnel-server/tests/admission.rs#L1277), and [storage executor evidence](td-022-storage-executor-evidence.md#L145).

## Reference designs and differences

| Reference | Sourced behavior | Runnel-relevant lesson and limit |
| --- | --- | --- |
| [NATS rolling upgrades](https://docs.nats.io/learn/deployment/rolling-upgrades) | Lame-duck mode rejects new client connections, transfers JetStream leadership, informs connected clients, waits a grace interval, then closes clients gradually. Its deployment guidance sizes the orchestrator grace period to include the whole drain. | Separating admission, client notification, work handoff, connection close, and process exit is useful. Runnel's current JSON-lines clients have no server-discovery or lame-duck handshake, and the local engine has no peer to receive work, so a single-node slice can only stop admission and close sockets after request handling. |
| [Apache Kafka graceful shutdown](https://kafka.apache.org/43/operations/basic-kafka-operations/#graceful-shutdown) | A graceful broker stop syncs logs and can move partition leadership to live replicas; controlled shutdown requires a surviving replica. | Syncing or waiting on local durable operations is distinct from leadership transfer. Runnel's local log syncs each durable append before success and has no replicas in this scope. Kafka's controlled handoff cannot be promised by a local-only shutdown. |
| [Apache Pulsar rolling restarts](https://pulsar.apache.org/docs/next/administration-rolling-restart/) | A broker deregisters, releases owned bundles, closes services, and exits. Per-bundle release and final service-close have separate limits, and the process manager must allow both to finish; force-stop can leave ownership to recover later. | A single deadline must account for every drain phase and external grace period. Bundle reassignment and ownership leases are cluster-specific mechanisms, not an Runnel single-node requirement. |
| [Kubernetes Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow) | The termination grace period includes preStop and process shutdown; the runtime sends TERM and later KILLs remaining processes. Terminating endpoints become not-ready for ordinary traffic while connection draining can continue. | Readiness withdrawal, draining, and a hard process kill are separate. Runnel must state its application deadline and leave external orchestration enough time; readiness 503 by itself does not prove endpoint removal or process completion. |
| [Tokio 1.53.1 blocking tasks](https://docs.rs/tokio/1.53.1/tokio/task/fn.spawn_blocking.html) and [runtime shutdown](https://docs.rs/tokio/1.53.1/tokio/runtime/struct.Runtime.html#shutdown) | A started spawn_blocking closure cannot be aborted. Runtime drop waits for it indefinitely; shutdown_timeout stops waiting but does not cancel the closure. | A cancelled request future is not proof the storage operation stopped. Runnel needs a deliberate choice between waiting for started local I/O and allowing the external hard deadline to terminate the process. |

NATS and Pulsar describe service handoff and traffic movement across a cluster; Kafka describes local log sync plus optional replica leadership migration. Those mechanisms depend on another live owner. Runnel's initial single-node outcome is narrower: stop receiving new work, make accepted durable outcomes explicit, and rely on normal local recovery after a forced stop. The references do not establish Runnel guarantees.

## Candidate outcome contract

The following is a recommendation for review, not accepted behavior.

1. **Enter draining once.** SIGINT and SIGTERM should take the same path. Mark readiness unavailable before closing the client listener so health probes and orchestrators can observe the transition. Keep liveness successful during the bounded drain; a deliberate stop is not a process-health failure. Keep metrics available during the drain when possible, and expose the shutdown phase and whether its deadline expired without high-cardinality labels.
2. **Stop admission.** Do not accept or dispatch a new client request after the drain transition. Close idle connections and partial frames promptly. Let only requests already dispatched to the local engine continue. Do not wait for clients to finish arbitrary idle sessions, retry loops, or new protocol frames.
3. **Use one absolute deadline.** Start one monotonic drain deadline when shutdown begins. Every listener and request wait consumes the same remaining time; a later phase must not receive a fresh full timeout. The configured request timeout may exceed the remaining shutdown budget, in which case shutdown can close the connection before the request has a response.
4. **Complete durable work when it can finish within the budget.** A clean shutdown waits for dispatched engine operations and response writes to finish within the deadline. It does not acknowledge a protocol success before the engine reports its durable success. It does not add a final log rewrite or alter the local durability boundary as a shutdown shortcut.
5. **Make deadline expiry observably different from a clean drain.** At expiry, close remaining client sockets and cancel async work that has not crossed into an uninterruptible blocking operation. Treat every request without a received response as an unknown client outcome; do not tell the client the operation was rolled back. Report the drain deadline as expired in logs/metrics and choose an exit-status policy explicitly. A zero exit status should not be mistaken for proof that all accepted work drained unless the implementation can establish that fact.
6. **State the external hard-stop boundary.** Started blocking filesystem calls cannot be safely cancelled by aborting their async caller. The application deadline therefore is not itself a hard process-exit guarantee. A deployment must allow the graceful drain to finish and retain an external hard deadline for a stuck process. If that external deadline sends a force kill, treat the stop as a crash at an arbitrary I/O boundary and verify recovery rather than claiming graceful completion.
7. **Preserve at-least-once recovery.** A publish or acknowledgement that committed but whose response was lost remains committed; the broker must not undo it to make shutdown look clean. A publish retry without a request identity can append another record; a publish retried with the current request identity can resolve to its prior offset. A repeated acknowledgement can resolve as already acknowledged. A delivered but unacknowledged message may be delivered again after restart. These are operation-specific unknown outcomes, not a promise that every timed-out operation failed.
8. **Keep local readiness distinct from availability.** Readiness becomes false as soon as drain begins and stays false until exit. Liveness remains true while the process is making bounded drain progress. Metrics remain scrapeable until the HTTP shutdown phase ends. These signals describe this process; they do not promise that a replacement is ready or that durable traffic is available elsewhere.

The fixed 25-second application timeout is an observed constant, not a recommended deployment value. The default request timeout is 30 seconds, and the illustrative Kubernetes manifest grants 30 seconds total termination time. The current five-second difference does not account for a stuck started blocking call or any preStop work. A future implementation should decide whether the graceful duration is configurable and coordinate its value with deployment grace periods; it should not treat 25 seconds as a portable hard exit guarantee.

## Alternatives and unresolved policy

| Option | Benefit | Cost or risk |
| --- | --- | --- |
| Close every connection immediately at signal time | Simple and fast; no idle or partial client can extend shutdown. | An already-dispatched operation may still finish in storage after its socket disappears, leaving a normal unknown outcome. It gives up a chance to return a result for fast operations. |
| Drain only already-dispatched requests, then close all sockets | Gives bounded local operations a chance to finish and matches the current single-request-at-a-time protocol handler. | Clients cannot discover another endpoint or receive a structured drain notice. Any operation that misses the shared deadline is ambiguous. |
| Add a separate pre-drain phase or administrative endpoint | Could give a load balancer time to stop routing before SIGTERM and could later support cluster-aware handoff. | Expands control-plane and deployment semantics; requires authorization, idempotency, endpoint-removal timing, and a clear relationship to signals. It is not needed to define the first local outcome. |

The second option is the recommended starting point. Resolve these choices in an ADR before implementation:

- Is the graceful duration fixed, configurable, or derived from the process manager's termination budget? If configurable, what lower and upper bounds keep it useful?
- Should deadline expiry produce a non-zero process exit, a distinct shutdown result, or both? How can logs and metrics distinguish timeout, listener failure, and completed drain?
- Does the service wait for started blocking I/O to settle, or does it rely on an external hard kill after a separate deadline? Tokio shutdown_timeout does not cancel the I/O and is not a substitute for this decision.
- Is it acceptable to close the connection after the storage operation commits but before its response is written? The outcome is inherently unknown to the client; no shutdown-specific retry response can be sent on a dead connection.
- Should readiness remain false throughout drain even if the drain later stalls? This proposal says yes; readiness is admission/traffic-removal state, not liveness.
- What metric names and log fields are sufficient to show shutdown phase, elapsed time, active requests, and deadline expiry without adding per-stream or per-client labels?

## Outcome and evidence gates

A local implementation may claim graceful shutdown only when real-process evidence establishes all of the following:

- SIGINT and SIGTERM enter the same tested drain path; readiness becomes unavailable before admission stops, liveness remains healthy during the drain, and the process eventually exits or reports deadline expiry.
- New TCP work is not dispatched after drain begins. Idle and partial-frame sockets close promptly; a request already dispatched follows the chosen policy and cannot hold the broker beyond its application deadline.
- A publish completing before the deadline returns its durable result. If its response is withheld or the deadline expires, restart shows either the committed record or a recoverable incomplete tail, with no claim of rollback. Exercise both a retry with request identity and the documented no-identity ambiguity.
- An acknowledgement completing before the deadline remains durable after restart. If the response is lost at the commit boundary, retry or restart demonstrates the persisted acknowledgement or safe redelivery; it never advances acknowledgement state before the durable update succeeds.
- A delivered but unacknowledged message is recoverable for at-least-once redelivery after shutdown/restart. Shutdown does not silently turn an unacknowledged delivery into a committed acknowledgement.
- Deadline tests distinguish an operation that is still waiting asynchronously from a started blocking filesystem call. Test harnesses must use unique ports and data directories, explicitly bound their own wait, and release or clean up FIFO/process resources on success and failure. Do not claim that aborting an async task cancels a started blocking call.
- Process exit status, log message, and metrics distinguish a completed drain from an expired deadline. If a test uses an external force kill, it asserts crash recovery, not clean shutdown.
- A deployment-level test verifies readiness propagation, the total termination budget including any preStop hook, and the external hard stop. The current 30-second Kubernetes grace value must be reassessed against the broker's application deadline and blocking-I/O behavior in a separate deployment-owned change.

The first local slice does not require a benchmark: it changes shutdown/recovery evidence and is not intended to improve throughput or latency. A diagnostic regression measurement may be useful if drain accounting changes active-request or connection scheduling, but it is not the primary gate.

## Explicitly outside the first local slice

- Raft peer admission, joining accepted peer RPC tasks, leader transfer, cluster-wide client routing, membership departure, quorum preservation, or rolling-restart sequencing.
- A client drain handshake, reconnect discovery, per-client notification, or graceful completion of a consumer's application-side processing.
- TLS, authentication, admin endpoints, deployment manifests, Kubernetes readiness propagation, service mesh behavior, or orchestration-specific grace-period configuration.
- Cancelling an already-started operating-system filesystem call, promising a process exit before an external force-kill deadline, or changing the storage durability boundary.
- Guaranteeing the outcome of an operation whose response was not observed by its caller.

## Planning and refactor assessment

No backlog or tech-debt update is proposed: the single-node deployment backlog already names shutdown/restart repeatability, and TD-022 already records the blocking-I/O shutdown bound and test gap. No ADR update is appropriate because this note proposes, but does not accept, behavior. No code refactor is proposed for a documentation-only change. Deployment grace alignment is a concrete follow-up gate, but deployment files are outside this note and need a separately owned implementation decision.

## Sources

- [NATS rolling upgrades and lame-duck mode](https://docs.nats.io/learn/deployment/rolling-upgrades), retrieved 2026-09-29.
- [Apache Kafka 4.3 graceful shutdown](https://kafka.apache.org/43/operations/basic-kafka-operations/#graceful-shutdown), retrieved 2026-09-29.
- [Apache Pulsar rolling restarts](https://pulsar.apache.org/docs/next/administration-rolling-restart/), retrieved 2026-09-29.
- [Kubernetes Pod termination flow](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow), retrieved 2026-09-29.
- [Tokio 1.53.1 spawn_blocking](https://docs.rs/tokio/1.53.1/tokio/task/fn.spawn_blocking.html) and [runtime shutdown](https://docs.rs/tokio/1.53.1/tokio/runtime/struct.Runtime.html#shutdown), retrieved 2026-09-29.
- [Axum 0.8.9 graceful-shutdown adapter](https://docs.rs/axum/0.8.9/axum/serve/struct.Serve.html#method.with_graceful_shutdown), retrieved 2026-09-29.
