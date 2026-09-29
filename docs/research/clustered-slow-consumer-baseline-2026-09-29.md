# Clustered slow-consumer delivery-window baseline (2026-09-29)

**Evidence class:** performance and benchmark evidence. This is a descriptive baseline for the existing `slow_consumer_backpressure` scenario; it reports no comparison or performance improvement.

## Scope and run conditions

This run exercises the current ordinary-consumer delivery window in the three-node clustered broker. Each independent case preloads 1,000 records, polls one record, polls again for the same consumer through another node before acknowledging, waits 10 ms, then acknowledges. The preload is outside the measured drain. The primary latency distribution covers poll plus acknowledgement request time and excludes both the configured processing delay and the second poll. Drain throughput and resource samples include the delay and duplicate-poll probe.

| Dimension | Value |
| --- | --- |
| Source revision | `93573fae6f8c9244704d34add167540fc46b31c6` |
| Topology and replication | 3 native broker processes; static three-node Multi-Raft; durable quorum commit plus local durable state |
| Client boundary | Public line-delimited JSON protocol, UTF-8 string payloads, no compression |
| Payloads and repetitions | 100 bytes × 3 complete runs; 1,024 bytes × 3 complete runs |
| Backlog per run | 1,000 preloaded records; preload excluded from the measured interval |
| Slow-consumer processing delay | 10 ms per record |
| Other configured runner settings | `--warmup 50` was passed as the matrix default; the probe preloads its own 1,000-record backlog outside measurement. Acknowledgement timeout: 30,000 ms; scenario timeout: 60 s. |
| Runtime and isolation | Native process runtime; six cases executed sequentially under the exclusive `/tmp/runnel-benchmark.lock` |
| Resource envelope | One Linux systemd user scope per case, covering benchmark client and all broker child processes; `CPUQuota=200%`, `MemoryMax=2G` |
| Build | `cargo build --locked -p runnel-server --release`, `CARGO_BUILD_JOBS=2`, fresh run-scoped target directory; Rust/Cargo 1.97.1 |
| Host | `rkth-precision5570`, Linux 7.0.0-34-generic x86_64, Python 3.14.4, 20 logical CPUs; runner processor-name field was empty |

The matrix command selected only `slow_consumer_backpressure`, with `--messages 1000 --warmup 50 --nodes 3 --ack-timeout-ms 30000 --payload-sizes 100,1024 --concurrency-values 2 --slow-consumer-delays-ms 10 --slow-consumer-timeout-seconds 60 --repetitions 3 --case-timeout-seconds 300 --max-cases 6 --native-resource-scope --cpus 2 --memory 2g --keep-going`. The matrix ran the first release build once before the first case's cluster child and reused that binary for the remaining five cases. The binary SHA-256 was `0675f7d8ea35f9e3ba71d5f7464cb54800d992c84bcedc31890be100fcb82123`.

The run used the supplied baseline without refreshing the branch. The coordinator checked the refreshed default branch before finalization; intervening changes were disjoint from the benchmark runner, benchmark README, and v2 artifact contract.

The benchmark host was checked immediately before execution: no local build, test, benchmark, or broker process was active; Docker had no running containers; the exclusive benchmark lock was available; load average was 0.03/0.07/0.10 with 19 GiB memory available. The systemd user target was active and a 2 CPU / 2 GiB scope preflight succeeded. The measured run ID is `20260929152812387517`; it ran from 2026-09-29 15:28:12.388 to 15:31:01.408 UTC. The matrix planned, attempted, and completed all six cases, with zero failures or timeouts.

The cold release build took 1m13s according to Cargo. Matrix case 1 took 89.061s overall, including that build; its clustered benchmark child ran from 15:29:25.942 to 15:29:41.390 UTC. The remaining case wall times, including cluster setup, preload, measured drain, and cleanup, were 15.430s, 15.424s, 16.579s, 16.177s, and 16.330s. The scenario's measured drain intervals were 14.447–14.775s. The child result does not split cluster startup, preload, and cleanup into separate elapsed fields.

## Per-run results

Primary and duplicate-poll latency values are `p50 / p99 / p99.9 / max`, in milliseconds. Each primary distribution contains 1,000 poll-plus-ack samples. Each duplicate-poll distribution contains 1,000 samples. Resource columns show broker-node aggregates sampled during the measured drain; memory and on-disk storage are MiB. The systemd limit also covers the client, but the scenario resource sampler reports broker-node resources rather than a separate client sample.

| Payload | Run | Drain throughput (msg/s) | Poll + ack latency p50 / p99 / p99.9 / max (ms) | Duplicate-poll latency p50 / p99 / p99.9 / max (ms) | Resource samples; CPU (s); avg / peak RAM (MiB); avg / peak storage (MiB) |
| --- | ---: | ---: | --- | --- | --- |
| 100 B | 1 | 67.68 | 3.18 / 4.45 / 4.69 / 5.21 | 1.22 / 1.79 / 2.04 / 2.07 | 147; 9.04; 52.19 / 53.04; 4.17 / 5.87 |
| 100 B | 2 | 67.86 | 3.14 / 4.39 / 4.65 / 5.06 | 1.20 / 1.83 / 2.00 / 2.02 | 147; 9.06; 50.87 / 52.18; 4.04 / 4.08 |
| 100 B | 3 | 67.75 | 3.16 / 4.32 / 4.50 / 4.54 | 1.21 / 1.83 / 1.93 / 2.15 | 147; 9.06; 51.72 / 52.66; 4.05 / 4.08 |
| 1 KiB | 1 | 69.22 | 2.87 / 8.54 / 9.74 / 9.89 | 1.09 / 7.63 / 10.49 / 10.60 | 144; 12.43; 192.39 / 210.28; 35.88 / 51.41 |
| 1 KiB | 2 | 68.83 | 2.93 / 8.80 / 11.56 / 13.41 | 1.10 / 7.37 / 9.96 / 10.93 | 145; 12.58; 194.22 / 204.54; 35.86 / 37.45 |
| 1 KiB | 3 | 68.93 | 2.92 / 8.48 / 9.37 / 9.91 | 1.09 / 7.20 / 10.38 / 10.40 | 144; 12.47; 189.06 / 203.34; 35.86 / 47.17 |

## Medians and observed ranges

The values below are medians across the three runs, followed by the observed minimum–maximum. They are descriptive summaries, not confidence intervals or stability tests.

| Payload | Throughput (msg/s) | Poll + ack p50 / p99 / p99.9 / max (ms) | Duplicate-poll p50 / p99 / p99.9 / max (ms) |
| --- | --- | --- | --- |
| 100 B | 67.75 (67.68–67.86) | 3.163 (3.145–3.181) / 4.390 (4.316–4.454) / 4.650 (4.496–4.687) / 5.058 (4.536–5.205) | 1.207 (1.198–1.223) / 1.827 (1.795–1.829) / 2.000 (1.925–2.037) / 2.066 (2.023–2.152) |
| 1 KiB | 68.93 (68.83–69.22) | 2.920 (2.871–2.927) / 8.538 (8.478–8.795) / 9.736 (9.370–11.561) / 9.913 (9.893–13.414) | 1.085 (1.085–1.101) / 7.370 (7.197–7.625) / 10.384 (9.957–10.493) / 10.605 (10.404–10.931) |

| Payload | Samples per run | Broker CPU seconds, median (range) | Average / peak aggregate RAM, median (range), MiB | Average / peak aggregate storage, median (range), MiB |
| --- | --- | --- | --- | --- |
| 100 B | 147 / 147 / 147 | 9.06 (9.04–9.06) | 51.72 (50.87–52.19) / 52.66 (52.18–53.04) | 4.05 (4.04–4.17) / 4.08 (4.08–5.87) |
| 1 KiB | 144 / 145 / 144 | 12.47 (12.43–12.58) | 192.39 (189.06–194.22) / 204.54 (203.34–210.28) | 35.86 (35.86–35.88) / 47.17 (37.45–51.41) |

The artifacts retain per-node CPU, resident-memory, and storage samples and their per-run averages and peaks. Aggregate storage is sampled data-directory footprint during the drain, not bytes written or write amplification. The aggregate resource values do not imply that the full configured 2 GiB was used.

## What this probe establishes

All six runs verified the existing ordinary-consumer window: a second poll through another node, before the first acknowledgement, returned the same offset and delivery attempt. Each result records 1,000 duplicate polls, 1,000 matches, `max_logical_in_flight_deliveries_observed: 1`, and `delivery_window_verified: true`. Redelivery was not expected.

The result does **not** demonstrate publisher throttling, request-admission rejection, or a configurable broker backpressure policy. It covers one bounded 1,000-record backlog, one consumer, two payload sizes, and a 10 ms application processing delay. Three repetitions are descriptive evidence only; they do not establish stable tail latency or a performance gain. The 1 KiB runs retain a wider p99.9 and maximum range than the 100 B runs, but this small sample is not enough to attribute the spread or infer a general payload-size effect. The benchmark does not exercise process failure, recovery, longer histories, or other consumer concurrency patterns.

## Artifacts and validation

The six child JSON results, six runner logs, eighteen broker logs, and matrix envelope are preserved under [`artifacts/slow-consumer-baseline-2026-09-29/`](artifacts/slow-consumer-baseline-2026-09-29/). The matrix envelope is [`matrix.json`](artifacts/slow-consumer-baseline-2026-09-29/matrix.json); [`artifact-manifest.json`](artifacts/slow-consumer-baseline-2026-09-29/artifact-manifest.json) records each raw and committed artifact's byte size and SHA-256. The 31 raw runner files total 1,054,108 bytes; their committed copies total 1,050,883 bytes. The raw matrix SHA-256 is `90c96fe249be1cbeda900e4c9fa9a6ae21921028ac3fba99fa06731e4dc24096`; its committed normalized copy SHA-256 is `fb23c293a3791a0cd0b7797ca84385d68b1df0edc158c217bc9518abc63d2327`. The artifact manifest SHA-256 is `c7de73c4d944534d7debee377ec77b24f12e0a08dde48b0ddca10c23d6109ffb`.

Raw runner outputs were kept in a unique temporary directory until the committed copies were validated. Only ephemeral path strings were normalized in the copies: the temporary build target to `$CARGO_TARGET_DIR`, temporary files to `$TMPDIR`, matrix/case outputs to their committed artifact paths, and the isolated worktree prefix to `<WORKTREE>`. Reversing exactly those path substitutions reproduced every original runner artifact byte-for-byte; measured values and other metadata were unchanged. The build target itself is not included in the artifacts.

All seven result JSON files parse as schema version 2. The matrix reports 6 planned, 6 attempted, 6 complete, and 0 failed cases; each child result is complete and contains the expected scenario, 1,000 latency samples, resource samples, and duplicate-poll verification fields. No local test suite was run; the assigned benchmark is the evidence. `git diff --check` and the report's artifact-link checks are part of the handoff validation.

## Project assessment and recommendation

**Expected effects and non-effects:** this adds a repeatable, source-pinned performance/behavior baseline and its raw machine-readable evidence. It changes no broker code, test behavior, configuration, protocol, or runtime performance, and supports no optimization claim.

**Backlog and tech-debt assessment:** the [clustered performance and fault baseline outcome](../backlog.md#establish-clustered-performance-and-fault-baselines) remains open. This run adds baseline values for an existing scenario but does not close its outstanding stable-tail, publisher-backpressure, or broader fault-coverage criteria. No backlog or tech-debt register update is warranted; the measured evidence and remaining scope are recorded here.

**Refactor assessment:** inspection of the matrix runner, scenario, resource-scope wrapper, and benchmark guidance found the existing sequential case isolation, bounded scope, run-scoped artifacts, explicit failure records, and scenario boundaries adequate for this evidence-only task. No concrete runner refactor is required by the baseline.

**Evidence gaps and risks:** the three-run samples are descriptive and do not establish stable tails; broker resource samples do not break out client consumption; sampled disk footprint is not write-cost attribution; publisher throttling and rejection remain untested; and one backlog size, delay, and consumer are covered. These gaps limit interpretation to the stated workload.

**Recommendation:** merge this documentation and artifact change after review and required pull-request checks pass. Rerun the same controlled workload before using it as a comparison point for a later change; do not infer a gain from these measurements alone.
