# Benchmark framework

## Concurrent local workflows

The benchmark commands are safe to run concurrently through the repository's
isolation runner:

```text
just isolated bench-container-smoke
just isolated bench-cluster-smoke
just isolated bench-cluster-container-smoke
just isolated bench-cluster-peer-forwarding-container-smoke
```

Each run receives a unique Cargo target directory, temporary directory, output
directory, and Docker network. Use the normal `just bench-*` commands for
single-run investigation or authoritative performance measurements; use
`just isolated bench-*` when the goal is to keep independent development runs
from sharing local state.

The repeatable local container benchmark is:

```text
just bench-container
```

It builds `runnel:bench`, starts one broker container, applies explicit CPU and memory limits, runs durable publish, concurrent publish, consume-and-acknowledge, publish/consume/acknowledge round-trip, and restart-recovery scenarios for 100-byte and 1-KiB payloads, then writes a JSON result under `benchmark-results/`.

The limits and workload can be changed without editing the runner:

```text
python3 scripts/benchmarks/run.py \
  --image runnel:bench \
  --cpus 2 \
  --memory 1g \
  --messages 10000 \
  --concurrency 8 \
  --payload-sizes 100,1024
```

Every raw runner emits the same version-2 result envelope. It records a unique run ID, exact command, full source and host provenance, explicit status and timestamps, workload and resource limits, target/runtime metadata, startup time, scenario-scoped cgroup CPU time, CPU efficiency, sampled resources, throughput, p50/p99/p99.9/max latency, logical payload throughput, and bounded deltas from the broker's `/metrics` endpoint where available. The single-node result stores its target under `backends.runnel`, just like clustered and comparison results.

Use `--scenarios` to run only the named scenarios when a benchmark consumer needs a smaller, explicit workload. The accepted names are `durable_publish`, `concurrent_publish`, `consume_ack`, `publish_consume_ack_roundtrip`, and `restart_recovery`; the default runs all of them. The native comparison adapter selects only `durable_publish` and `consume_ack`, because those are the scenarios it retains from the Runnel result.

This is an end-to-end benchmark of the current development protocol. It is not yet a fair Kafka, Redpanda, or NATS JetStream comparison: those brokers require adapters that express equivalent acknowledgement, replication, ordering, and delivery guarantees. The comparison work belongs in the benchmark backlog. Do not compare raw numbers across brokers until the adapter semantics and environment are recorded in the result.

The benchmark and product-fit harnesses use the negotiated v2 Protobuf protocol through a persistent Python socket client. The client supports stream creation, scalar and batch publish, scalar and grouped poll/acknowledgement, and replay. It does not implement TLS, bearer authentication, batch poll/acknowledgement, consumer-policy operations, or the general runnelctl configuration surface. Native benchmark processes bind the application listener to loopback. Container benchmark runs pass the explicit insecure-development listener flag only inside the per-run Docker network and publish ports on host loopback; this is development-only plaintext and does not model a secure deployment. The HTTP listener is a separate unauthenticated surface.

The short `just bench-container-smoke` recipe is used by CI to verify that the image can start, accept workload traffic under limits, expose metrics, and recover an unacknowledged message. It is a workflow check, not a performance gate.

## Clustered baseline

Run the real three-node clustered baseline with:

```text
just bench-cluster
```

The runner defaults to native broker processes with independent durable directories and exercises the public protocol through multiple nodes. It measures durable publish, non-grouped consume/acknowledge, a bounded slow-consumer backlog drain, sequential shared-consumer delivery, parallel shared-consumer delivery, restart recovery, and retained-data recovery for the selected payload sizes. Each result records the node count, acknowledgement timeout, quorum durability boundary, protocol boundary, workload, throughput, p50/p99/p99.9 latency, aggregate and per-node broker CPU time, resident memory, and on-disk storage samples. Results use the same `backends` shape as the single-node container and comparison runners, so they can be normalized into the existing history dashboard.

The opt-in `raft_log_growth` scenario sends a bounded, fixed number of records to one stream through sequential public `publish_batch` requests, samples each node's current data-group Raft entry count and purge index plus the persisted Raft log, state-machine journal, checkpoint, and snapshot file sizes, then waits for an actual snapshot/purge before restarting a follower and verifying replay and acknowledgement of offset 0. Setup publishes offset 0 outside the measured interval. Each measured record must return a published outcome at its expected contiguous offset; a failed or incomplete response stops the case without retry because some records in that request may already have committed. Batch outcomes remain per-record; a request does not imply an atomic batch or one Raft entry. For example:

```text
python3 scripts/benchmarks/cluster.py \
  --scenarios raft_log_growth \
  --raft-log-growth-messages 256 \
  --raft-log-growth-batch-size 8 \
  --raft-log-growth-observation-every 8 \
  --raft-log-growth-cycle-timeout-seconds 30 \
  --payload-sizes 1024 \
  --output benchmark-results/raft-log-growth.json
```

`--raft-log-growth-messages` accepts 64 through 4,096 records (default 256), `--raft-log-growth-batch-size` accepts 1 through the protocol limit of 1,024 records (default 1), and `--raft-log-growth-observation-every` accepts 1 through 1,024 records (default 8). The measured logical payload volume is capped at 16 MiB; this keeps every encoded batch request below the protocol's 64 MiB request limit even at the maximum batch record count. `--raft-log-growth-cycle-timeout-seconds` bounds the snapshot/purge wait to 1 through 300 seconds (default 30).

The measured interval reports record throughput and p50/p99/p99.9 batch-request latency; latency samples are per request, and a final partial batch is allowed when the message count is not divisible by the requested batch size. The artifact records the requested size, actual batch-size counts, outcome-validation boundary, observation samples, and how much interval time was spent reading persisted paths. Samples occur after completed batches that reach or pass the next configured publish interval, so the recorded message index is the actual boundary. Use a wider interval to reduce observation work or a narrower interval for more detail, and interpret the observed peak as a sampled lower bound. Resource samples include per-node CPU, memory, and total data-directory footprint. File sizes and net footprint deltas identify which persisted paths grow or compact; they do not measure bytes written or physical write amplification. Snapshot build metrics are aggregate per node, while purge indices and file sizes come from the selected stream's data group. Raft entries count consensus commands; the harness does not infer a one-to-one relationship between entries, requests, and broker messages. These are workload observations through real compaction cycles, not an accepted commit-cost bound. Collect them under the exclusive benchmark lock with bounded, recorded CPU and memory resources, and compare only runs with matching runtime, topology, payload size, message count, batch size, observation interval, durability, and resource limits.

#### Opt-in persistence-write attribution

The `persistence-write-counters` feature adds fixed-role application-side write counters to the three-node Raft broker. The role and operation set is bounded, carries no stream/group labels, and is absent from the default build. Only the explicit `GET /metrics?persistence_write_counters=true` request adds these metrics; ordinary `/metrics` scrapes and default benchmark artifacts keep their existing shape. The `raft_log_growth` scenario requests counter snapshots at measured publish/purge boundaries and records follower node 3's pre-stop values, fresh post-restart baseline, and recovery delta separately. It marks the process reset and never subtracts counters across it.

Build both server variants from the same checkout and retain each binary before rebuilding the other variant. Run from the repository root; `cluster.py --build` uses default features, so use `--binary` and omit `--build` for these runs. Set `TMPDIR` to the ext4-backed benchmark directory when `/tmp` is tmpfs so broker data uses the same intended storage for both variants:

```sh
RUN_DIR="$(pwd)/benchmark-results/td026-write-counters"
mkdir -p "$RUN_DIR/bin" "$RUN_DIR/tmp" "$RUN_DIR/ext4-artifacts"
cargo build --locked --release -p runnel-server
cp target/release/runnel "$RUN_DIR/bin/runnel-default"
cargo build --locked --release -p runnel-server --features persistence-write-counters
cp target/release/runnel "$RUN_DIR/bin/runnel-counters"
```

For an enabled/disabled diagnostic, create a small shell script that invokes this command six times with unique `--output`, `--log-dir`, and systemd `--unit` values. Keep the exclusive lock around all six alternating runs, in this order: default/counters for pair 1, counters/default for pair 2, default/counters for pair 3. This prevents another benchmark from entering between pairs while alternating order limits simple time/order effects.

```sh
systemd-run --user --scope --collect --unit=runnel-td026-pair1-off \
  --property=CPUQuota=200% --property=MemoryMax=2G -- \
  env TMPDIR="$RUN_DIR/tmp" \
  RUNNEL_BENCHMARK_CPU_LIMIT=2 RUNNEL_BENCHMARK_MEMORY_LIMIT=2G \
  python3 scripts/benchmarks/cluster.py \
  --binary "$RUN_DIR/bin/runnel-default" \
  --runtime process --nodes 3 --scenarios raft_log_growth \
  --raft-log-growth-messages 256 --raft-log-growth-batch-size 8 \
  --raft-log-growth-observation-every 8 \
  --raft-log-growth-cycle-timeout-seconds 30 --payload-sizes 1024 \
  --output "$RUN_DIR/ext4-artifacts/pair1-off.json" \
  --log-dir "$RUN_DIR/ext4-artifacts/pair1-off-logs"
```

Change the binary, artifact names, and systemd unit for each invocation; use the same workload and resource values for all six. Do not rebuild between those invocations. Record the checkout revision and any source diff, host/kernel/filesystem and storage location, systemd limits, exact command order, and all six raw artifacts. The `write_bytes_offered` total is application input offered to `write_all`; `write_bytes_completed` credits only successful calls, while a failed call increments the unknown-prefix count. Neither field is physical device traffic. The fixed roles cover Raft-log rewrite, journal append/compaction, checkpoint, and snapshot persistence. Metadata/manifest writes, startup/recovery repair, open/create and directory creation, local-core storage, filesystem metadata/writeback, and device-level traffic are outside these counters. Keep file footprint and optional cgroup/device totals separate. Treat the enabled/disabled difference as diagnostic instrumentation overhead only, not as a product cost bound or performance claim.

`matrix.py` can repeat the log-growth probe across measured publish counts, payload sizes, batch sizes, observation intervals, and independent repetitions. For a compact diagnostic matrix that also varies retained broker history and the general public append-batch baseline, run under the exclusive benchmark lock with bounded native resources:

```text
python3 scripts/benchmarks/lock.py \
  --path /tmp/runnel-benchmark.lock \
  --mode exclusive -- \
  python3 scripts/benchmarks/matrix.py \
  --scenarios raft_log_growth,retained_hot_path,publish_batch \
  --messages 256 \
  --payload-sizes 100,1024 \
  --retained-message-values 1025,2048 \
  --raft-log-growth-message-values 64,256 \
  --raft-log-growth-observation-every-values 8,32 \
  --raft-log-growth-cycle-timeout-seconds 30 \
  --raft-log-growth-batch-size-values 1,8 \
  --batch-size-values 1,8 \
  --repetitions 2 \
  --max-cases 48 \
  --case-timeout-seconds 300 \
  --native-resource-scope --cpus 2 --memory 2g \
  --output benchmark-results/td026-growth-matrix.json
```

The matrix writes one raw result and runner log per case and retains each child result in the matrix envelope. Each `raft_log_growth` case keeps its measured record count fixed while varying payload size, requested batch size, observation interval, and repetition; `--raft-log-growth-batch-size-values` expands each batch size into a fresh cluster case. The growth-message count drives log production but is not an exact retained-entry target because snapshot/purge can advance during measurement. The observation interval changes file-sampling frequency, so include its measured observer time/fraction when comparing results. `retained_hot_path` varies preloaded broker-message history, while `publish_batch` remains a separate general batching baseline. These cells are repeatable workload coverage, not an aggregate performance claim. The sampled Raft log byte count is the serialized file's current length; journal, checkpoint, and snapshot values are sampled file lengths, and node storage samples are sampled totals of file lengths. These observations do not count cumulative bytes written, allocated disk blocks, or device writes, and they do not attribute journal or snapshot write work per commit. The snapshot-build counter is aggregate per node, not a byte or per-commit measure.

The opt-in `snapshot_build_hot_path` scenario records build-duration metrics and
samples process RSS while ordinary durable publishes run against a known
retained state. Setup preloads one stream through the public protocol and is
excluded from publish latency. The default is 256 measured publishes over
2,048 retained records; `--snapshot-build-messages` accepts 64 through 4,096,
`--retained-messages` accepts 1,025 through 16,384 for this scenario, and
combined logical payload volume is capped at 16 MiB. The bounded build-cycle
wait defaults to 30 seconds and accepts 1 through 300 seconds. For example:

```text
python3 scripts/benchmarks/cluster.py \
  --scenarios snapshot_build_hot_path \
  --snapshot-build-messages 256 \
  --retained-messages 2048 \
  --snapshot-build-cycle-timeout-seconds 30 \
  --payload-sizes 100 \
  --output benchmark-results/snapshot-build-hot-path.json
```

The metrics endpoint exports per-process aggregates over registered Raft
groups, with no group, stream, peer, or consumer labels; the local engine omits
these clustered diagnostics. Started, completed, failed, in-progress, duration
count/sum, and process-lifetime maximum are reported; duration includes the
builder's state-lock wait, encoding, snapshot persistence, journal compaction,
and cache publication. Returned failures contribute to duration count/sum;
cancelled calls increment `builds_started` and clear the in-progress gauge but
do not contribute a duration sample or a completed/failure count. Scrape
aggregation adds four atomic reads per group to the existing `O(groups)`
snapshot-metric walk. The group count is not capped by this probe.

The result requires a newly completed successful build and records its
per-process deltas. Its request latency and throughput cover measured publishes
only; the resource window also includes the bounded completion wait through an
idle-build boundary. While this scenario is selected, the existing 100 ms
resource sampler scrapes the active-build gauge alongside per-node RSS and
separately counts samples taken during the publish loop. Zero overlapping
samples means the sampler may have missed a shorter build, not that no overlap
occurred. The maximum RSS at a sampled active-build observation is a lower
bound, not peak or incremental snapshot memory; observer duration is reported
because metrics scrapes add load. This is a bounded build probe, not
snapshot-install, transfer, cold-recovery, or performance-improvement evidence.
`matrix.py` can sweep `--snapshot-build-message-values` and
`--retained-message-values` into independent cases, with payload sizes and
repetitions retained as separate dimensions.

The opt-in `publish_batch` scenario measures the clustered public `publish_batch` protocol path, which is not part of the default workload. Setup creates the stream and publishes the warmup records outside the measured interval. Measured requests contain up to 32 records by default, rotate persistent clients across the cluster nodes, and validate one published outcome and contiguous offset for every input record. Set `--batch-size` from 1 through the protocol's 1,024-record limit for one focused case. To repeat a comparable batch-size matrix with independent artifacts, use `matrix.py --scenarios publish_batch --batch-size-values 1,8,32`; each batch size becomes a separate case and is never combined with another size. The result counts records for throughput and uses one latency sample per batch request, recording `batch_size`, batch count, outcome validation, setup exclusion, and the latency scope in scenario metadata. This is a clustered batching baseline, not evidence that the current engine commits a batch atomically or that it performs one consensus append per request; compare only runs with matching batch size, payload, message count, topology, runtime, and resource limits.

Run the same three-node workload with one bounded Docker container per broker:

```text
just bench-cluster-container
```

This selects `--runtime container`, applies the `--cpus` and `--memory` limits to each broker, keeps the benchmark client on the host, and connects the brokers over a private Docker network. The containerized and native modes share the workload and result schema, but their numbers are separate measurements because process scheduling, networking, filesystem mounts, and resource boundaries differ. Use `--runtime container` directly with `cluster.py` to select a custom image, workload, or resource budget. The three-node container run is a Runnel cluster baseline, not a cross-product ranking; the competitor adapter remains a separate suite.

The clustered runner uses the broker's 30-second acknowledgement-timeout default. This keeps a constrained benchmark host from interpreting scheduling or quorum delay as a delivery failure during throughput measurements. Override it with `--ack-timeout-ms` when intentionally measuring redelivery behavior.

The slow-consumer scenario preloads a finite stream, polls one message at a time, waits for the configured processing delay, and then acknowledges it. Its request-latency samples cover only the broker poll and acknowledgement requests; its drain throughput and resource interval include the intentional delay. The default delay is 10 milliseconds and is required to be shorter than the acknowledgement timeout, so the scenario measures a slow but successful consumer rather than deliberate redelivery. Set the delay with `--slow-consumer-delay-ms`; the bounded message count and payload size remain the workload limits. This is evidence about behavior under a slow consumer, not proof that the current broker applies a configurable backpressure policy.

The opt-in `slow_consumer_backpressure` scenario probes the current delivery-window behavior. It preloads the same finite backlog, polls one record, issues a second poll for the same consumer through another node before acknowledging, and verifies that the second poll returns the same in-flight record. Its result records the duplicate-poll count, delivery-window verification, duplicate-poll latency, bounded timeout, and scenario-scoped CPU, memory, and storage samples. The primary request-latency samples exclude processing delay and the duplicate probe; drain throughput and resources include both. This establishes bounded duplicate-delivery suppression for the current ordinary-consumer path. It does not exercise publisher throttling, a request-admission rejection, or a configurable broker backpressure policy, so it cannot establish those guarantees. Use `--slow-consumer-timeout-seconds` to set its bounded wall-clock budget (60 seconds by default, 300 seconds maximum), and include `slow_consumer_backpressure` in `--scenarios` when running it.

The retained-data recovery scenario is named `cluster_retained_recovery`. It preloads a separate stream with 2,048 records by default, which is above the current local 1,024-record tail-index boundary, then excludes that setup from the measured interval. The latency timer starts immediately before restarting one node and ends after polling, verifying, and acknowledging the earliest retained record at offset 0, then closing the recovery client. Set the retained history size with `--retained-messages`; values must be at least 1,025. The scenario runs once for the first selected payload size and records `retained_messages`, `retained_logical_payload_bytes`, `latency_scope`, and `resource_sample_scope` in its existing v2 scenario metadata. The resource sample window starts before the recovery operation and ends after it returns, so it brackets the latency interval and includes result construction after acknowledgement and client close. Earlier numeric samples also cover the full pre-restart-through-acknowledgement-and-close interval even though their metadata described only readiness through acknowledgement; they must not be interpreted as post-readiness samples. A future post-readiness measurement needs a distinct scope. This measures recovery and cold replay with a known retained-data size; it does not measure retention cleanup, disk-pressure admission, batching, or prove that recovery cost is bounded. Compare runs only when retained count, payload size, topology, durability, runtime, and resource limits match.

The opt-in `retained_hot_path` scenario measures the append path after retained state already exists. It preloads exactly `--retained-messages` records into a fresh stream, excludes that setup, and then publishes the selected `--messages` through persistent public clients on the same stream while checking contiguous post-preload offsets. Select it explicitly, for example:

```text
python3 scripts/benchmarks/cluster.py \
  --scenarios retained_hot_path \
  --messages 1000 \
  --retained-messages 2048 \
  --payload-sizes 100 \
  --output benchmark-results/retained-hot-path.json
```

The result is a normal schema-v2 scenario record named `cluster_retained_hot_path`; its latency and throughput cover only measured durable publish round trips after the preload, while metadata records the retained count and logical payload bytes. This is a diagnostic hot-path baseline, not an optimization claim. It does not measure consume/replay, restart recovery, retention cleanup, disk-pressure admission, storage amplification, or behavior beyond the selected retained-history sizes. Compare only matching retained count, payload, measured message count, topology, durability, runtime, resource limits, and source/build conditions.

The opt-in `leader_failure_recovery` scenario is a bounded fault baseline for one three-node quorum. Select it explicitly with a run-scoped output and log directory:

```text
run_dir="$(mktemp -d -t runnel-leader-failure-XXXXXX)"
python3 scripts/benchmarks/cluster.py \
  --build \
  --scenarios leader_failure_recovery \
  --messages 1 \
  --payload-sizes 100 \
  --leader-failure-timeout-seconds 60 \
  --output "$run_dir/result.json" \
  --log-dir "$run_dir/logs"
```

The setup creates a stream and commits one record through node 1, then excludes that work from measurement. The current static clustered implementation starts node 1 with `--bootstrap` before the other nodes and uses it to initialize the metadata group. The scenario treats node 1 as the assumed initial leader on that basis; the provisional public protocol forwards follower requests and exposes no leader identity. It stops node 1, retries public requests through both surviving endpoints until they complete, restarts node 1 on its run-scoped port and durable directory, then verifies publish and poll requests through the restarted endpoint and acknowledgement through a surviving endpoint. These observations establish public request success through those endpoints; they do not identify the elected leader or prove which process handled a forwarded request. Retried publishes use stable `request_id` values so an ambiguous response can be retried without intentionally creating a second record.

The default fault budget is 60 seconds and the maximum is 300 seconds. The scenario runs once for the first selected payload size and records the failed and surviving node IDs, bootstrap-based initial-leader assumption, public request endpoint IDs by operation, verified offsets, request attempts, restart-ready time, scenario resource samples, and `/metrics` delta. Its fixed three-record sequence is deliberately separate from the general sustained `--messages` workload setting. Because one node is restarted during the measured interval, its metric counters can reset; the result marks that condition as expected. The single latency sample spans process stop through survivor endpoint requests, restarted-endpoint publish and poll, and acknowledgement through a survivor endpoint. This is a reliability/recovery measurement, not a throughput comparison or evidence of a runtime performance improvement. The bounded scope excludes network partitions, storage loss, membership changes, two-node failures, direct observation of leader identity, repeated stable tail-latency measurements, and cross-engine comparisons. Use `--skip-recovery` to omit this scenario along with the other restart/recovery probes.

The opt-in `follower_failure_recovery` scenario uses the same bounded public-protocol sequence but stops node 2, a non-bootstrap follower, before survivor publish/consume/acknowledgement and same-node restart. It records `failure_state: follower_process_stop` and the selected failed node. Comparing the bootstrap-assumed-leader and follower process stops shows whether public requests complete and recover in each case; it does not directly observe an election or identify a leader. Neither scenario is a complete partition, storage-loss, or multi-failure matrix.

For a rerunnable workload and fault matrix, use:

```text
just bench-cluster-matrix
```

`matrix.py` expands scenarios, payload sizes, relevant concurrency and slow-consumer delay values, retained-history sizes, runtimes, general `publish_batch` sizes, Raft log-growth record counts, batch sizes and observation intervals, snapshot-build publish counts, and repetitions into independent sequential `cluster.py` invocations. Every case receives a unique result and broker-log directory under the selected artifacts directory. The default matrix covers durable publish, consume/acknowledge, slow-consumer drain, restart/replay, retained-history recovery, and both leader and follower process-stop probes. Select `slow_consumer_backpressure` explicitly to expand each `--slow-consumer-delays-ms` entry into an independent delivery-window case; select `retained_hot_path` explicitly to expand each `--retained-message-values` entry into a separate post-preload publish case; select `publish_batch` with `--batch-size-values` to expand each general publish batch size into a separate case; select `raft_log_growth` to expand `--raft-log-growth-message-values`, `--raft-log-growth-batch-size-values`, and `--raft-log-growth-observation-every-values` into independent cluster-probe cases; select `snapshot_build_hot_path` to expand `--snapshot-build-message-values` and `--retained-message-values` across the selected payload sizes. Growth cases retain the same measured message count across batch-size values, and each selected payload and batch size receives separate provenance and artifacts. Set `--raft-log-growth-cycle-timeout-seconds` to bound waiting for its snapshot/purge observation and `--snapshot-build-cycle-timeout-seconds` to bound waiting for a successful snapshot build; keep the outer `--case-timeout-seconds` long enough for cluster startup, measurement, and scenario waits. The growth count is a measured workload size, not a promise that the same number of Raft entries remain retained after purge. These opt-in dimensions keep slow-consumer, retained-state, batching, log-growth, and snapshot-build coverage visible without changing the existing default matrix. `--batch-size` remains the single-size compatibility option and defaults to 32 for `publish_batch`. Use `--keep-going` to retain later cases after a failure; the command still exits nonzero and records failed or timed-out cases in the matrix envelope. `--case-timeout-seconds` is an outer bound, while each fault scenario keeps its own bounded recovery timeout. Matrix cases are diagnostic coverage and are not combined into an authoritative performance claim.

The matrix accepts `--runtimes process,container`, `--cpus`, and `--memory` for explicit resource dimensions. Container cases enforce per-broker Docker limits. Add `--native-resource-scope` on Linux to place native broker and client processes in the same bounded systemd user scope. Cluster resource samples include aggregate and per-node CPU, resident memory, and on-disk storage bytes; storage scans are throttled between scenario boundaries so they do not turn the sampler into a hot-path observer. Cases with different dimensions are intentionally not aggregated into a performance ranking; normalize and aggregate matching raw case results when repeated evidence is needed.

For a small lifecycle check of the matrix orchestration:

```text
just bench-cluster-matrix-smoke
just isolated bench-cluster-matrix-smoke
```

The opt-in `peer_forwarding` scenario targets the topology-free forwarding pool. Select it explicitly so the existing clustered entrypoint keeps its established workload:

```text
python3 scripts/benchmarks/cluster.py \
  --build \
  --scenarios peer_forwarding \
  --messages 256 \
  --warmup 16 \
  --payload-sizes 100 \
  --peer-forwarding-concurrency 8 \
  --peer-forwarding-stream-count 4 \
  --peer-response-delay-ms 5 \
  --peer-forwarding-timeout-seconds 60 \
  --output benchmark-results/peer-forwarding.json
```

`--peer-forwarding-stream-count` is bounded from 1 to 64 and defaults to one. Each stream maps to a data group. The setup creates all selected streams and publishes `--warmup` records to each through node 1; this work is excluded from measurement. The measured `--messages` value remains the total across all streams, distributed round-robin, and the selected stream count cannot exceed that total. The result validates contiguous offsets independently within every stream. Increasing stream count therefore keeps measured publish work fixed while increasing data-group count and setup work; it is a data-group-density characterization baseline, not a transport-strategy comparison.

Each measured publish uses one persistent public client per worker on node 2, the non-bootstrap ingress node, and therefore exercises the broker's internal `Forward` request to the data-group leader. The default eight workers exceed the current four shared forwarding permits (five pooled connections minus one reserved control connection), making pool wait visible when peer responses are delayed. Offsets are validated for benchmark correctness and are not exposed as a public product guarantee.

`--peer-response-delay-ms` enables a run-scoped native TCP proxy on every peer address. The proxy forwards the real framed peer protocol and delays only `Forward` responses, leaving Raft control responses on the same transport but outside the injected delay; it is deliberately a bounded perturbation for response-delay and saturation experiments, not a production topology. A zero delay keeps direct peer connections. The proxy is native-process-only because container peers cannot reach the host loopback proxy. Keep the delay small enough for the cluster's acknowledgement and request timeouts. The focused scenario has a bounded wall-clock budget from `--peer-forwarding-timeout-seconds` (default 60 seconds, maximum 300); individual protocol requests retain the broker's 30-second timeout.

The result uses the normal schema-v2 envelope and records the selected scenarios, message and warmup counts, payload sizes, forwarding concurrency, stream/data-group count, response delay, timeout, runtime, resource limits, full source revision, host provenance, and public-protocol durability boundary. Its `cluster_peer_forwarding` record reports total measured publishes, stream count, aggregate setup warmup count, per-stream measured message range, throughput, logical payload throughput, p50/p99/p99.9/maximum follower round-trip latency, aggregate and per-node CPU/memory samples, and `/metrics` deltas. The scenario metadata identifies the forwarding ingress, operation, setup and latency boundaries, delay, concurrency, and saturation interpretation. When enabled, raw backend metadata at `backends.runnel-cluster.peer_response_proxy` additionally reports proxy connections, framed requests/responses, delayed responses, and per-node listen/target ports. These counters include cluster startup and setup traffic; use the scenario latency and metric deltas for measured comparisons.

The `metadata.peer_connection_census` field records per-node broker-owned established TCP socket endpoint counts after setup warmup and after measured forwarding. On Linux it joins each broker process's `/proc/<pid>/fd` socket inodes to `/proc/<pid>/net/tcp` and `tcp6`, counting only endpoints whose local port is that broker's peer-listener port or whose remote port is a configured peer destination. Container runs resolve the broker's host PID with `docker inspect` and first try that host-PID procfs census. If host procfs ownership is inaccessible, a bounded `docker exec` reads PID 1's procfs from inside the container, after verifying the exec UID matches the broker's effective UID. Each boundary and node records the observation source, scope, availability, count semantics, and observation duration; inaccessible tables, identity mismatches, or probe timeouts yield an explicit unavailable result rather than an inferred count. Counts are per-process socket endpoints, not unique inter-node connections: a connection can appear once at each broker. They include all established traffic on the matched peer endpoints and do not attribute sockets to a Raft group or distinguish replication, snapshot, and compatibility traffic. These are settled-boundary snapshots, not an atomic cluster-wide sample, and their observation duration is outside the measured forwarding interval.

This probe establishes a repeatable forwarding, overload, and bounded data-group-density baseline; it is not evidence of a runtime performance improvement. Compare only runs with the same native runtime, topology, payload, stream count, warmup per stream, total measured message count, forwarding concurrency, response delay, timeout, resource budget, and source/build conditions. The delayed forwarding responses still include the target's normal quorum work, so a result does not isolate pool wait from consensus or target-processing cost. The public clustered benchmark remains unchanged unless `peer_forwarding` is selected in `--scenarios`.

The sequential `matrix.py` runner can expand this dimension with `--peer-forwarding-stream-count-values 1,4,16`. It creates one independent case per stream count and retains each result artifact. Keep the matrix small enough to fit `--max-cases` and the outer `--case-timeout-seconds` budget.

`--skip-recovery` skips restart and failure-recovery scenarios, including the retained-data recovery, leader-failure, and follower-failure probes; it does not skip the independent `retained_hot_path` publish probe. The cluster's temporary durable directories, generated stream names, native ports, process/container resources, and container network are run-scoped. Supply distinct output and log paths when invoking the script directly; use the isolation runner when independent process-heavy workflows overlap.

The clustered entrypoint remains `cluster.py` for command compatibility. Its private implementation boundaries are `cluster_cli.py` for argument validation and scenario dispatch, `cluster_lifecycle.py` for process/container ownership and cleanup, `cluster_scenarios.py` for workload and recovery behavior, `cluster_resources.py` for resource limits and observation, `cluster_faults.py` for bounded fault injection, and `cluster_results.py` for schema-v2 result composition. This is a structural refactor; it does not change workload defaults, result fields, or benchmark interpretation.

The scheduled GitHub Actions history uses 200 messages per clustered scenario by default, independently of the native comparison workload. The retained-data probe remains at its separate 2,048-record default unless `--retained-messages` is overridden. This keeps repeated recovery and quorum measurements within the workflow time budget on the workflow runner's constrained CPU allocation while retaining enough traffic to compare the cluster scenarios. Increase the `cluster_messages` input for a larger manual run when investigating sustained-load behavior.

For a quick lifecycle check:

```text
just bench-cluster-smoke
```

For an isolated process-level check of the multi-stream forwarding scenario:

```text
just bench-cluster-peer-forwarding-smoke
```

For a container lifecycle check, use `just bench-cluster-container-smoke`. To check multi-stream peer forwarding and direct socket visibility inside broker containers, use:

```text
just bench-cluster-peer-forwarding-container-smoke
```

That Linux+Docker workflow samples broker-owned established peer socket endpoints after setup warmup and after measured forwarding, and fails unless both samples include available non-negative counts for all three nodes with direct procfs provenance. Host-PID procfs is tried first; Docker exec may collect the broker's own procfs when host access is restricted, and the artifact records which route was used. Neither smoke workflow is a performance gate. Host scheduling, background processes, filesystem, kernel state, Docker networking, and container resource enforcement can materially affect the numbers. Keep the host and workload metadata with any result used for comparison.

## Profiling

Capture Linux CPU call-graph samples while the cluster is under a sustained publish/consume/acknowledge workload:

```text
just profile-cluster
```

The profile workflow writes one `perf.data` file and one `perf report --stdio` text report per broker process, plus broker logs and a JSON manifest, under `benchmark-results/profile-*/`. Use the reports to distinguish time spent in protocol parsing, serialization, consensus, locks, storage, and scheduling. The workload is deliberately representative rather than a synthetic microbenchmark; change its duration, worker count, payload size, and sampling frequency through the script options when investigating a hypothesis. `perf` permissions and kernel configuration are host prerequisites, so this workflow remains local and optional.

For internal stage timing without `perf`:

```text
just profile-cluster-instrumented
```

The `instrumentation` Cargo feature compiles timing guards into the broker and `RUST_LOG=runnel::timing=trace` records their durations in each node's broker log. The workflow summarizes p50, p99, and maximum microseconds for protocol handling, lock waits, storage, quorum operations, state-machine application, and peer RPCs in `profile.json`. Peer RPCs are split into connect, write, and read stages. The profile workload performs one publish, poll, and acknowledgement per completed message, so stage counts can be interpreted alongside the recorded workload count. The normal build has no timing guards, and the timing feature should not be used for uncontaminated performance comparisons.

## First-pass broker comparison

Run the native-tool comparison with:

```text
just bench-compare
```

The runner starts each selected broker in isolation on a temporary Docker network, applies the same per-container broker and client CPU/memory limits, creates one stream/topic, publishes 10,000 messages, consumes them, records image identifiers and scenario-scoped resource measurements, and writes a JSON result under `benchmark-results/compare-<timestamp>.json`. The default payload sizes are 100 bytes and 1 KiB. Container names, data directories, and the Docker network are run-scoped, so separate comparison invocations can overlap without name collisions; cleanup removes the containers, temporary data, and network on success or failure. Broker readiness is bounded at 45 seconds, with each Docker readiness probe bounded at 10 seconds; measured native commands retain a 180-second timeout.

Each raw result is self-describing: `run_id` identifies the artifact, `command` records the exact invocation, `source` records the full revision and CI workflow identity, and `environment` records the host, platform, processor, Python version, and CPU count. The per-backend records retain the pinned broker image and resolved image identifier; the measurement-client image and acknowledgement/replication boundary are documented in the backend metadata and command implementation. This provenance must travel with any numbers used for comparison.

The raw result also contains a machine-readable `comparison_guardrail` with `apples_to_apples: false` and `ranking_eligible: false`. Every backend has `semantic_metadata` for its acknowledgement boundary, replication topology, measurement boundary, client identity, and measured scenario classes. Each scenario carries `metadata.comparison_class`: `publish-only`, `consume-with-ack`, or `consume-without-ack`. The harness validates these declarations and fails before writing a result when a boundary or classification is missing or inconsistent. This keeps the native measurements useful as engineering baselines while making their non-equivalence explicit to downstream tooling.

The pinned images are Apache Kafka `4.3.1`, Redpanda `v26.2.1`, NATS Server `2.14.5-alpine`, and `nats-box` `0.19.7`. The Runnel image is built by the `just` recipe. Redpanda's development mode needs more than a 1 GiB cgroup, so the shared default is 2 CPUs and 2 GiB; pass `--cpus` and `--memory` to change it.

This is intentionally a first baseline built around native benchmark clients. Runnel and JetStream report durable publish latency; Kafka and Redpanda use Kafka's native producer performance client, whose latency includes its configured client batching, and their native consumer performance client reports fetch throughput without application-level acknowledgement. The JSON records these boundaries and marks the output as non-equivalent. Do not present the single-node output as a final cross-product ranking until a common client workload and equivalent consumer acknowledgement path exist.

The comparison entrypoint is kept stable at `compare.py`; its shared lifecycle and resource cleanup, semantic result policy, backend execution, and CLI/result-envelope responsibilities have focused private ownership in `compare_lifecycle.py`, `compare_results.py`, `compare_backends.py`, and `compare_cli.py`. This is a maintainability extraction only: it does not make the native workloads equivalent or change the comparison guardrails.

The bounded three-node slice measures only replicated durable publish for the three external competitors:

```text
python3 scripts/benchmarks/compare.py \
  --nodes 3 \
  --backends kafka,redpanda,nats \
  --messages 1000 \
  --payload-sizes 100 \
  --cpus 2 \
  --memory 2g \
  --client-cpus 2 \
  --client-memory 2g
```

Run the replicated three-node competitor baseline with:

```text
just bench-compare-cluster
```

This records one publish scenario per payload size for Kafka, Redpanda, and NATS JetStream with replication factor three. It is deliberately a separate `cluster-comparison` history suite: the current Runnel cluster runner measures the public protocol and consumer acknowledgement paths, while this first competitor slice has only equivalent durable-publish adapters. The weekly competitor workflow repeats and aggregates this suite independently from the Runnel optimization history.

`--nodes 3` starts three Kafka KRaft brokers/controllers, three Redpanda brokers, or three NATS servers with JetStream clustering. Kafka topics use one partition, replication factor 3, `min.insync.replicas=3`, `acks=all`, and producer idempotence. JetStream streams use file storage and `--replicas=3`; the native synchronous publisher measures the PubAck boundary. Each broker container receives the broker limits and the short-lived native client receives the client limits, so a three-node run consumes up to three times the per-container broker budget plus the client budget. The result keeps per-node resource summaries and aggregate CPU/memory fields.

The three-node mode rejects Runnel because this comparison runner has no distributed Runnel adapter, and it omits consumers because Kafka and Redpanda's native consumer performance client does not perform application-level acknowledgements. It therefore establishes a useful RF=3 publish baseline, not a complete end-to-end or failure-tolerance comparison. The broker modes also differ in their exact persistence and acknowledgement implementation: `acks=all`/`min.insync.replicas=3` and synchronous JetStream PubAck are recorded as client-visible boundaries, not a claim that every broker performs identical filesystem flushes. Fault injection, common client code, partitioning/concurrency parity, and replicated consume/ack remain follow-up work.

Examples:

```text
python3 scripts/benchmarks/compare.py --backends kafka,redpanda --messages 1000 --payload-sizes 100 --cpus 2 --memory 2g
python3 scripts/benchmarks/compare.py --backends nats --messages 10000 --payload-sizes 1024 --cpus 2 --memory 2g
```

## History and dashboard

Normalize a comparison result and generate local history data:

```text
python3 scripts/benchmarks/normalize.py \
  --input benchmark-results/compare-<timestamp>.json \
  --output benchmark-results/normalized.json
python3 scripts/benchmarks/build_history.py \
  --runs benchmark-results \
  --output benchmark-results/site
```

The normalized schema intentionally excludes native tool logs. It retains the version-2 run envelope, workload, limits, image identifiers, semantic boundaries, scenario resource and server-metric deltas, source revision, workflow provenance, and measured points. `build_history.py` aggregates these records into `site/data.json` on the generated `benchmark-history` branch. The hand-authored HTML, CSS, and JavaScript in `docs/benchmarks/` are served directly by GitHub Pages and read that public history data from the raw GitHub URL. The longer Runnel-only history is the primary optimization series; native and three-node competitor records are kept as separate suites. Invalid or unrelated JSON files are skipped.

The dashboard uses the dedicated Runnel suite as the primary optimization history. Older Runnel points recorded by the native-comparison workflow remain available as a separate history, because their measurement boundary can differ. Charts keep benchmark suite, backend, operation, and payload size in separate visual series, so selecting all sizes or suites does not connect unrelated measurements or hide which point belongs to which workload. Raw run medians are shown as dots, while a five-run rolling median makes the direction of change easier to see; repetition ranges are shown as a band when available.

## Local performance evidence

The policy for deciding whether a change needs a benchmark, interpreting stable or inconclusive results, diagnosing outliers, and reporting findings lives in [docs/benchmarking.md](../../docs/benchmarking.md). This README documents the harnesses, workload semantics, options, result schema, and history generation that support that policy.

Use `just bench-pr-local` for the authoritative same-host current-versus-`origin/main` comparison. When a complete comparison is inconclusive, use `just bench-pr-local-until-stable` to retain attempts while retrying the controlled workflow. Use `just bench-pr-local-quick` only for diagnostics; it is not performance evidence.

The generated Markdown report and raw JSON artifacts remain under `benchmark-results/pr-local/`. Preserve them with the handoff so the exact revision, workload, resources, repetition, stability result, and measurement boundaries remain reviewable.
