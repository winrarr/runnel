# Container benchmarks

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

The result records the image, source revision, host information, resource limits, startup time, scenario-scoped cgroup CPU time, CPU efficiency, sampled container memory, workload parameters, throughput, and p50/p99/p99.9/max latency where a scenario has per-message latency.

This is an end-to-end benchmark of the current development protocol. It is not yet a fair Kafka, Redpanda, or NATS JetStream comparison: those brokers require adapters that express equivalent acknowledgement, replication, ordering, and delivery guarantees. The comparison work belongs in the benchmark backlog. Do not compare raw numbers across brokers until the adapter semantics and environment are recorded in the result.

The short `just bench-container-smoke` recipe is used by CI to verify that the image can start, accept workload traffic under limits, expose metrics, and recover an unacknowledged message. It is a workflow check, not a performance gate.

## First-pass broker comparison

Run the native-tool comparison with:

```text
just bench-compare
```

The runner starts each selected broker in isolation on a temporary Docker network, applies the same broker and client CPU/memory limits, creates one stream/topic, publishes 10,000 messages, consumes them, records image identifiers and scenario-scoped resource measurements, and writes a JSON result under `benchmark-results/compare-<timestamp>.json`. The default payload sizes are 100 bytes and 1 KiB.

The pinned images are Apache Kafka `4.3.1`, Redpanda `v26.2.1`, NATS Server `2.14.5-alpine`, and `nats-box` `0.19.7`. The Runnel image is built by the `just` recipe. Redpanda's development mode needs more than a 1 GiB cgroup, so the shared default is 2 CPUs and 2 GiB; pass `--cpus` and `--memory` to change it.

This is intentionally a first baseline built around native benchmark clients. Runnel and JetStream report durable publish latency; Kafka and Redpanda use Kafka's native producer performance client, whose latency includes its configured client batching, and their native consumer performance client reports fetch throughput without application-level acknowledgement. The JSON records these boundaries. Do not present the output as a final cross-product ranking until a common client workload and equivalent consumer acknowledgement path exist.

Examples:

```text
python3 scripts/benchmarks/compare.py --backends kafka,redpanda --messages 1000 --payload-sizes 100 --cpus 2 --memory 2g
python3 scripts/benchmarks/compare.py --backends nats --messages 10000 --payload-sizes 1024 --cpus 2 --memory 2g
```

## History and dashboard

Normalize a comparison result and generate a local dashboard:

```text
python3 scripts/benchmarks/normalize.py \
  --input benchmark-results/compare-<timestamp>.json \
  --output benchmark-results/normalized.json
python3 scripts/benchmarks/build_site.py \
  --runs benchmark-results \
  --output benchmark-results/site
```

The normalized schema intentionally excludes native tool logs. It retains workload, limits, image identifiers, semantic boundaries, scenario resource samples, source revision, workflow provenance, and measured points. The GitHub benchmark workflow stores these normalized records and `site/data.json` on the generated `benchmark-history` branch. GitHub Pages serves the static dashboard from the separate `benchmark-pages` branch; its JavaScript reads the public history data from the raw GitHub URL. Local raw Runnel-only and comparison JSON files can also be read by the site generator; invalid or unrelated JSON files are skipped.

To generate only the static page or only history data:

```text
python3 scripts/benchmarks/build_site.py \
  --runs benchmark-results \
  --output benchmark-results/page \
  --index-only \
  --data-url https://raw.githubusercontent.com/winrarr/runnel/benchmark-history/site/data.json
python3 scripts/benchmarks/build_site.py \
  --runs benchmark-results \
  --output benchmark-results/data \
  --data-only
```
