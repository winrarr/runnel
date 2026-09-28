# TD-026 isolated Raft log persistence baseline

Status: initial lower-level performance evidence; the clustered commit-cost bound remains open

Measured: 2026-09-28

Code baseline: [`b97901ebf86ab7594756b2e34ef1b9b519abff20`](https://github.com/winrarr/runnel/commit/b97901ebf86ab7594756b2e34ef1b9b519abff20) (exact `ci.yml` run [36461610629](https://github.com/winrarr/runnel/actions/runs/36461610629) passed)

Measurement harness: [`7b70b8d2e847a3bb2068446922dcf6d1e5d8fe37`](https://github.com/winrarr/runnel/commit/7b70b8d2e847a3bb2068446922dcf6d1e5d8fe37)

## Finding

The current `LogStore` persistence path has measurable cost growth as retained serialized Raft history grows, especially with 1 KiB commands. In this isolated storage measurement, a paired append flush and committed-index update took a median 32.6 ms with no retained entries and 75.8 ms with 4,096 retained entries for a single 1 KiB entry; a 32-entry batch took 33.9 ms and 96.2 ms respectively. The paired logical JSON output grew from 8,768 bytes to 35,172,644 bytes for the single-entry case and to 35,438,810 bytes for the 32-entry case.

The timing result is local to `LogStore` and does not bound a clustered client commit. The configured OpenRaft policy requests a snapshot after 32 logs since the prior snapshot and retains four logs, but this harness deliberately did not build snapshots or call purge. Its 36-entry and larger inputs are synthetic no-purge stress points; they are not observed steady-state retention or product limits. For 100-byte commands, fixed sync cost dominated over the measured range: paired medians increased by about 10% from zero to 4,096 entries. The byte counts still grew with the retained map.

## Measurement boundary

The ignored test `log_store::tests::measure_retained_log_append_persistence_cost` calls the real `RaftLogStorageExt::blocking_append` on the current `LogStore`, which awaits OpenRaft's log-flush callback. It then calls `RaftLogStorage::save_committed` with the appended last log ID. That preserves append-before-commit persistence ordering and measures two actual log-store persistence operations per sample. The path being timed includes serialization, temporary-file creation, `write_all`, file `sync_all`, rename, directory `sync_all`, and the append callback. The two measured file lengths are summed as the pair's logical JSON bytes written.

Each sample reloads a prebuilt log with a fixed number of valid `Publish` entries outside the timer. It then appends the selected batch and persists the committed index. The preload contains no purged index; it is setup, not measured work. The test writes and reads only `raft-log.json`. It does not construct a `StateMachineStore`, write its apply journal or snapshot, start Raft, contact peers, run the snapshot policy, or invoke `purge`. Consequently, the byte counts are the exact serialized file lengths passed to `atomic_write` for this path, not device-level physical write amplification. No end-to-end cluster latency, recovery time, p99/p99.9, or CPU attribution is established.

The current settings in `crates/runnel-raft/src/group_manager.rs` are `SnapshotPolicy::LogsSinceLast(32)` and `max_in_snapshot_log_to_keep = 4`. They describe the intended snapshot trigger and retention after compaction; they are not a hard bound if snapshotting or purge is delayed or cannot progress. The measured 4, 32, and 36-entry points are therefore useful configuration landmarks only. In particular, 36 entries were seeded directly and were not produced by one observed snapshot cycle.

## Workload and resources

- Baseline code was `b97901e`; the test-only harness was committed as `7b70b8d` before the measured run. No production runtime code or persisted format changed.
- Retained pre-append log entries: `0,4,32,36,256,1024,4096`.
- Command payload sizes: `100,1024` bytes. Append batch sizes: `1,8,32` entries.
- Each of the 42 dimension cells had two warmups and 20 measured samples. The raw CSV contains the separate append and `save_committed` durations and file lengths for all 1,680 observations.
- The measurement held the exclusive repository benchmark lock and ran in a Linux systemd user scope limited to 2 CPUs and 2 GiB. The host was a 12th Gen Intel Core i9-12900H with Linux `7.0.0-34-generic`; the benchmark directory was on ext4 backed by the local encrypted volume, not `/tmp` tmpfs.
- The measurement test completed successfully in 41.27 seconds after a 25.29-second release build. The raw [sample CSV](td-026-log-store-persistence-baseline.csv) preserves all observations.

The rerunnable command from the repository root is:

```sh
python3 scripts/benchmarks/lock.py --mode exclusive --no-wait -- \
  systemd-run --user --scope --collect --unit=runnel-td026-log-cost \
  --property=CPUQuota=200% --property=MemoryMax=2G -- \
  env RUNNEL_TD026_OUTPUT_DIR=/path/on/local-ext4/td026-run \
    RUNNEL_TD026_RETAINED=0,4,32,36,256,1024,4096 \
    RUNNEL_TD026_BATCHES=1,8,32 \
    RUNNEL_TD026_PAYLOADS=100,1024 \
    RUNNEL_TD026_WARMUPS=2 RUNNEL_TD026_SAMPLES=20 \
  cargo test --locked --release -p runnel-raft --lib \
    log_store::tests::measure_retained_log_append_persistence_cost \
    -- --ignored --exact --nocapture --test-threads=1
```

Use a new output directory for each run. The exclusive lock uses the repository's default `/tmp/runnel-benchmark.lock`; `--no-wait` makes a concurrent benchmark fail instead of silently joining it.

## Results

Each row reports the median and observed min–max of the paired append and commit-index durations across 20 samples. `Logical bytes` is the sum of the resulting JSON file lengths from those two separate rewrites. Times are microseconds.

| Retained entries | Payload bytes | Batch entries | Logical bytes | Pair median (µs) | Pair observed range (µs) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 100 | 1 | 1,376 | 33,032.5 | 32,333.0–33,664.7 |
| 0 | 100 | 8 | 9,650 | 32,659.8 | 31,943.8–36,148.4 |
| 0 | 100 | 32 | 38,107 | 33,056.7 | 30,067.2–39,685.5 |
| 0 | 1024 | 1 | 8,768 | 32,576.9 | 31,830.0–33,453.5 |
| 0 | 1024 | 8 | 68,786 | 32,754.1 | 31,794.5–33,973.6 |
| 0 | 1024 | 32 | 274,651 | 33,943.1 | 33,586.6–34,919.0 |
| 4 | 100 | 1 | 6,146 | 32,813.0 | 30,853.2–33,789.0 |
| 4 | 100 | 8 | 14,429 | 32,689.0 | 31,104.7–34,551.9 |
| 4 | 100 | 32 | 42,893 | 31,914.2 | 29,582.5–34,345.0 |
| 4 | 1024 | 1 | 43,106 | 32,829.5 | 31,978.7–38,952.4 |
| 4 | 1024 | 8 | 103,133 | 33,992.7 | 32,058.9–35,059.2 |
| 4 | 1024 | 32 | 309,005 | 33,933.7 | 29,521.0–34,682.3 |
| 32 | 100 | 1 | 39,336 | 32,736.7 | 31,768.0–33,410.7 |
| 32 | 100 | 8 | 47,638 | 32,803.4 | 32,161.9–37,268.7 |
| 32 | 100 | 32 | 76,102 | 33,241.5 | 32,342.3–34,457.2 |
| 32 | 1024 | 1 | 283,272 | 33,822.5 | 31,347.5–34,603.4 |
| 32 | 1024 | 8 | 343,318 | 33,804.1 | 32,746.9–35,379.9 |
| 32 | 1024 | 32 | 549,190 | 34,664.9 | 33,374.1–35,819.0 |
| 36 | 100 | 1 | 44,080 | 32,585.8 | 29,969.7–34,368.9 |
| 36 | 100 | 8 | 52,382 | 33,379.6 | 27,760.4–37,437.7 |
| 36 | 100 | 32 | 80,846 | 32,406.1 | 31,095.7–46,520.3 |
| 36 | 1024 | 1 | 317,584 | 34,111.0 | 32,321.0–35,176.2 |
| 36 | 1024 | 8 | 377,630 | 34,538.9 | 30,362.2–35,691.8 |
| 36 | 1024 | 32 | 583,502 | 35,059.4 | 30,545.9–35,492.7 |
| 256 | 100 | 1 | 305,630 | 34,320.6 | 33,247.0–35,138.2 |
| 256 | 100 | 8 | 313,960 | 32,670.1 | 29,954.3–33,465.9 |
| 256 | 100 | 32 | 342,520 | 33,073.4 | 28,392.0–34,034.8 |
| 256 | 1024 | 1 | 2,205,374 | 36,174.8 | 35,338.1–37,569.3 |
| 256 | 1024 | 8 | 2,265,448 | 36,407.3 | 31,971.8–38,125.1 |
| 256 | 1024 | 32 | 2,471,416 | 35,952.4 | 34,580.2–38,288.0 |
| 1024 | 100 | 1 | 1,219,652 | 35,383.5 | 33,325.7–41,096.6 |
| 1024 | 100 | 8 | 1,228,010 | 34,924.4 | 32,721.7–35,948.9 |
| 1024 | 100 | 32 | 1,256,666 | 35,790.8 | 32,170.1–36,753.0 |
| 1024 | 1024 | 1 | 8,796,452 | 38,593.5 | 36,704.3–44,790.4 |
| 1024 | 1024 | 8 | 8,856,554 | 38,818.1 | 37,780.4–40,915.2 |
| 1024 | 1024 | 32 | 9,062,618 | 39,330.4 | 37,024.5–42,272.8 |
| 4096 | 100 | 1 | 4,887,620 | 36,567.7 | 34,220.1–42,779.6 |
| 4096 | 100 | 8 | 4,895,978 | 36,831.7 | 35,639.1–46,668.0 |
| 4096 | 100 | 32 | 4,924,634 | 36,505.0 | 35,428.1–41,503.7 |
| 4096 | 1024 | 1 | 35,172,644 | 75,778.4 | 74,136.1–91,584.2 |
| 4096 | 1024 | 8 | 35,232,746 | 77,331.8 | 75,002.3–119,080.7 |
| 4096 | 1024 | 32 | 35,438,810 | 96,246.0 | 94,708.8–97,988.3 |

The raw CSV contains separate operation observations; the table's pair ranges retain the same sample number when adding append and `save_committed`. These 20-sample ranges are descriptive, not confidence intervals or p99 estimates. For larger retained histories, the size of each complete rewrite increasingly dominates the fixed sync cost. With small payloads, observed time growth is much smaller and less consistent even though the full serialized file is still rewritten.

## Disposition and next evidence

This measurement answers the narrow storage question: yes, the full-map rewrite produces measurable local cost growth as retained encoded history increases. It does not establish an accepted cost bound for clustered commits, and it does not demonstrate that a normal live cluster retains 36 entries or more after snapshot and purge behavior.

Keep [TD-026](../tech-debt.md#td-026-raft-log-persistence-rewrites-retained-entries) open. The next evidence should run the real three-node benchmark through actual snapshot and purge cycles, correlate the live retained Raft-log count with committed append batches and payload sizes, and report end-to-end latency and recovery. Separate Raft-log writes from state-machine journal and snapshot work using an opt-in, per-path measurement boundary; include device-level write and resource data when available. Do not infer a product limit or redesign storage from this isolated microbenchmark.

This measurement answers the narrow storage question: yes, the full-map rewrite produces measurable local cost growth as retained encoded history increases. It does not establish an accepted cost bound for clustered commits, and it does not demonstrate that a normal live cluster retains 36 entries or more after snapshot and purge behavior.

Keep [TD-026](../tech-debt.md#td-026-raft-log-persistence-rewrites-retained-entries) open. The next evidence should run the real three-node benchmark through actual snapshot and purge cycles, correlate the live retained Raft-log count with committed append batches and payload sizes, and report end-to-end latency and recovery. Separate Raft log writes from state-machine journal and snapshot work using an opt-in, per-path measurement boundary; include device-level write and resource data when available. Do not infer a product limit or redesign storage from this isolated microbenchmark.
