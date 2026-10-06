# TD-026 isolated Raft log persistence baseline

Status: fixed-role persistence attribution and one counter-enabled/disabled diagnostic are complete; the clustered commit-cost bound remains open

Measured: isolated persistence baseline on 2026-09-28; live clustered samples on 2026-09-29; counter-enabled/disabled diagnostic on 2026-10-06

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

## Live clustered sample

On 2026-09-29, the opt-in `raft_log_growth` process scenario exercised a three-node static Multi-Raft cluster through the public line-delimited JSON protocol. Each run made one setup publish, excluded from timing, then 256 sequential durable-quorum publishes to a dedicated stream. It recorded one live per-node Raft and state-machine file-state sample every eight publishes, waited for an actual snapshot and purge-index advance, then restarted follower node 3 and verified the earliest retained payload at offset 0 through poll and acknowledgement. The two runs used 100-byte and 1,024-byte payloads, respectively (run IDs `20260929124859229798` and `20260929124932218934`). The server binary came from code revision `49652a19cbd11fe68f79c602df3522a42dfaceba`; the benchmark harness was the in-progress `raft_log_growth` implementation in this change. The raw runner metadata's `source.revision` is the worktree HEAD and does not identify a separate harness commit.

The host was a 12th Gen Intel Core i9-12900H with Linux `7.0.0-34-generic`, 20 logical CPUs, and ext4 on the local encrypted volume. The systemd user scope covered the client and three broker processes with a 200% CPU quota and 2 GiB memory limit. The cluster acknowledged durable quorum commits; batching was disabled and compression was not used.

| Payload | Throughput (msg/s) | p50 / p99 / p99.9 / max publish latency (ms) | Observer I/O share of measured interval | Peak sampled retained entries per node | Final retained entries; purged through | Net per-node file-size deltas: Raft log / state journal / checkpoint / snapshot |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 B | 934.8 | 0.810 / 1.358 / 5.428 / 6.801 | 16.9% | 32 | 8; 251 | +3,883 / +1,293 / 0 / +354,752 B |
| 1,024 B | 350.6 | 1.690 / 3.362 / 11.876 / 14.397 | 32.3% | 40 | 8; 251 | +29,759 / +12,381 / 0 / +3,160,025 B |

All nodes recorded eight completed snapshot builds and eight observed purge-index advances in each run. After the cycle, restarting node 3 became ready in 53.3 ms (100 B) and 108.2 ms (1,024 B); both runs replayed and verified the earliest retained payload at offset 0, then acknowledged it. Raw run artifacts preserve the complete workload, resource samples, per-node observations, and recovery metadata: [100-byte JSON](td-026-cluster-log-growth-100b.json), [1-KiB JSON](td-026-cluster-log-growth-1kib.json), and [tabular per-node observations](td-026-cluster-log-growth-observations.csv).

These are two single, descriptive runs. The observer itself accounted for 16.9% and 32.3% of the measured interval, so throughput and latency are materially affected by observation overhead and cannot be treated as uncontended cost estimates. The sampled peaks of 32 and 40 retained entries are lower bounds on the peaks between observations, observed only for this 256-publish workload and these payload sizes. They do not establish a general retention bound or supported workload limit. File-size deltas describe persisted path footprints before and after the interval; they do not measure serialized bytes written, device-level writes, or per-path I/O time. Raft log entry counts describe consensus history and are not broker message counts.

## Write attribution and remaining measurement boundaries

The 2026-10-06 repeated matrix added payload and requested batch-size coverage, but its sampled file lengths do not show how many bytes each persistence path submitted or how many block writes reached storage. That earlier matrix reports 5.3%–35.5% observer I/O; it is distinct from the final counter-enabled/disabled diagnostic below, where the file observer used 0.62%–0.91% of the measured interval. These are observer-cost diagnostics, not publish-cost evidence. The methods below separate three signals that must not be conflated: bytes submitted by Runnel, current file footprint, and I/O accounted below the filesystem.

The relevant write sites are distinct. `LogStore::persist` serializes the complete retained log map and sends it through an atomic temporary-file write, file sync, rename, and parent-directory sync. State-machine apply appends each journal record as a length-prefixed JSON frame and calls `sync_data`. Snapshot and checkpoint persistence use atomic rewrites; snapshot building also compacts the journal by reading retained entries, writing a temporary journal, syncing it, renaming it, and syncing the directory. These paths are in `crates/runnel-raft/src/log_store.rs`, `crates/runnel-raft/src/state_machine_journal.rs`, `crates/runnel-raft/src/state_machine_store.rs`, and `crates/runnel-raft/src/lib.rs`.

| Method | What it measures and attributes | Limits and cost | Fit for Runnel |
| --- | --- | --- | --- |
| Fixed application counters at persistence call sites | Per-role serialized output and bytes offered to the journal frame writer or atomic-write helper, operation outcomes, and elapsed time for serialization, write, sync, rename, and directory-sync stages. The caller knows whether the target is the Raft log, state journal, checkpoint, or snapshot. | Counts application submissions, not filesystem metadata, delayed writeback, or device writes. With `write_all`, input length is an exact completed submission only when the call succeeds; a failed call can have written a prefix. The enabled/disabled check is inconclusive for overhead. [`Write::write_all`](https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all) retries partial writes until the buffer is accepted or an error occurs. | Implemented first slice: narrow, path-aware, portable across the broker's supported platforms, and distinguishes five fixed persistence roles. Uses fixed role and operation enums, with no stream/group labels or event logs. |
| Syscall tracing (`strace` or `perf trace`) | Returned byte counts and call duration for `write*`, `pwrite*`, `fsync`, `fdatasync`, and rename syscalls. With open/close and descriptor tracking, it can validate application counters and expose extra writes made by the process. [`strace`](https://man7.org/linux/man-pages/man1/strace.1.html) can filter by syscall, PID, file descriptor, and path; [`perf trace`](https://man7.org/linux/man-pages/man1/perf-trace.1.html) supports syscall summaries and cgroup selection. | Captures the process-to-kernel boundary, not subsequent asynchronous writeback or device traffic. A pathname may become a temporary name and then be renamed; tracing must maintain descriptor/path state and include all relevant syscalls. `strace` uses [`ptrace`](https://man7.org/linux/man-pages/man2/ptrace.2.html); its own manual provides a syscall-overhead adjustment, so detailed tracing can perturb the workload and trace timings need calibration. `perf trace` depends on perf-event access policy and kernel/tool support. Use it as a bounded validation run, not as the primary timed result. | Useful for checking that path counters cover actual syscalls, especially temp-file, sync, rename, and directory operations. It will include unrelated process writes unless filtered and attributed carefully. |
| Kernel VFS/filesystem tracing or eBPF aggregation | Kernel events can count or time writes below the syscall layer and can be filtered by process/cgroup. The kernel [event tracing](https://docs.kernel.org/trace/events.html) interface exposes available tracepoints through tracefs, and tools such as ftrace and perf can consume them. | Tracepoint availability and fields vary by kernel/filesystem; VFS function probes can be less stable than tracepoints. Path resolution across file descriptors, temporary files, rename, and writeback complicates per-file grouping. Event capture, buffers, and access controls add operational overhead; verify privileges and lost-event counters. The kernel [perf access policy](https://docs.kernel.org/admin-guide/perf-security.html) may restrict tracing to privileged users/capabilities. It still does not prove which application operation caused a later merged or delayed block request. | Defer until app counters and a cheaper system boundary disagree or leave a specific question unanswered. It is a diagnostic escalation, not needed to establish submitted bytes by path. |
| File footprint (`st_size`, optionally allocated blocks) | Start/end logical length for each persisted path; on Linux, `st_blocks` can add allocated-block footprint, as described by the Linux [`stat`](https://man7.org/linux/man-pages/man2/stat.2.html) interface. This extends the current `file_bytes` samples with a separate footprint field. | A footprint is a point-in-time property, not cumulative writes. Rewriting an equal-sized file can produce zero net change; atomic rename replaces the old inode, and the temporary file may be gone by the next sample. `st_size` is logical length, while allocated blocks and filesystem accounting have different semantics. | Keep as a low-frequency side measurement (start/end or snapshot boundaries), not an attribution substitute. Remove repeated JSON file reads from timed samples where possible. |
| cgroup `io.stat` and block-device statistics | cgroup v2 [`io.stat`](https://docs.kernel.org/admin-guide/cgroup-v2.html#io-interface-files) provides per-device read/write bytes and I/O counts for a cgroup. [`/sys/block/<dev>/stat`](https://docs.kernel.org/block/stat.html) reports completed requests and sectors; its sector counters use 512-byte sectors. A dedicated cgroup can associate block I/O with a Runnel process group; device counters provide an aggregate cross-check. | cgroup counters aggregate all cgroup I/O, not paths, and buffered-writeback attribution requires filesystem support. The kernel documents support for ext2, ext4, btrfs, f2fs, and xfs; on other filesystems writeback can be charged to the root cgroup, and concurrent writers to a shared inode can be misattributed. Device counters are system-wide and include unrelated activity, filesystem metadata, and journal writes. Layered devices such as encrypted volumes need a declared accounting layer; summing both mapped and backing-device counters can count the same logical I/O twice. Neither signal is NAND/media write amplification. | Use `io.stat` per node only when cgroup v2, the I/O controller, filesystem writeback attribution, and process isolation are confirmed. Otherwise record block-device deltas only on an otherwise idle host and label them system-wide. Treat these as filesystem/device-boundary totals, never per-path totals. |

Runnel already has optional [`instrumentation`](../../crates/runnel-raft/Cargo.toml) `StageTimer` spans for `raft.log_append`, `raft.state_persist`, and `raft.state_checkpoint`. They cover broad methods, but do not report byte/operation counts or split serialization, write, sync, and rename stages. The timing guards in [`runnel-engine`](../../crates/runnel-engine/src/lib.rs) emit a trace event per method call, and the benchmark [profiling guidance](../../scripts/benchmarks/README.md#profiling) warns that these timings should not be used for uncontaminated performance comparisons. The new counter slice records bounded aggregate counts and elapsed time without adding per-operation trace output.

Application-side counters are implemented behind the `persistence-write-counters` feature. The fixed roles are `raft_log_rewrite`, `state_machine_journal_append`, `state_machine_journal_compaction`, `state_machine_checkpoint`, and `state_machine_snapshot`; fixed operations are `serialize`, `snapshot_state_serialize`, `write_all`, `sync_data`, `sync_all`, `rename`, and `directory_sync`. Counters aggregate per process/node and never include stream or group identifiers. Each role/operation cell reports attempts, successes, failures, cumulative elapsed nanoseconds, bytes offered to `write_all`, bytes from successful `write_all` calls, failed calls with unknown accepted prefix, and serialization output bytes. Serialization output is separate from bytes offered to the writer. On a failed `write_all`, the full offered input is recorded as offered, no completed bytes are credited, and an unknown-prefix event is recorded.

The counters are compiled out unless `persistence-write-counters` is enabled. In that build only, `GET /metrics?persistence_write_counters=true` exports the bounded role/operation series; the default metrics request and the benchmark's ordinary metrics scrape remain unchanged. Snapshots are cumulative per-process values and relaxed atomic reads, so the individual fields are not a transactionally consistent point-in-time view if persistence is concurrent with a scrape. The benchmark captures snapshots at workload boundaries and treats a restarted process as a new counter epoch.

The five roles cover the Raft-log atomic rewrite, state-machine journal append and compaction, checkpoint write, and snapshot write. They intentionally exclude Raft group metadata and manifest writes, other startup/recovery repair, file open/create and directory creation, network output, local `runnel-core` persistence, and any operation outside these five call-site roles. `write_all` bytes describe application bytes offered and successfully accepted by the Rust writer interface; they do not describe allocated blocks, delayed writeback, filesystem metadata, or physical device bytes. File footprint remains a separate point-in-time field, and cgroup/device totals remain optional separately labeled host measurements.

The counters expose attempt counts and cumulative duration for serialization, data/all sync, rename, and directory sync where each role uses those stages. They do not create per-operation event output. The bounded enabled/disabled comparison below used the same worktree source, workload, resources, storage, and durability settings. It is a diagnostic only; it is not a product cost bound or performance conclusion.

For the enabled/disabled diagnostic, build and retain default-feature and `persistence-write-counters` binaries from the same checkout, omit the benchmark runner's default-feature `--build` path, and alternate the two variants under the exclusive lock. Set `TMPDIR` to the same ext4-backed local data area for every run, and pass the systemd CPU/memory limits through the existing benchmark resource metadata environment. The 2026-10-06 comparison below follows this procedure. Capture `io.stat` around each node only if the benchmark can place each broker in a separate cgroup and the filesystem/controller support is confirmed; also record the exact major:minor device and whether it is the filesystem-facing mapped device or a backing device. If this is unavailable, a before/after `/sys/block/<dev>/stat` read on an idle host is a separately labeled whole-device cross-check, not path attribution.

Use syscall tracing only for a short calibration run that checks the path counters and operation coverage, and keep it out of the headline timed comparison. Correlate traces to workload phases with start/stop markers; verify the trace and application timestamp clocks before joining their timestamps. Even with aligned clocks, interval totals do not establish which writeback or block completion was caused by a specific application call. [`fsync`/`fdatasync`](https://man7.org/linux/man-pages/man2/fsync.2.html) boundaries are particularly useful to retain: Linux documents that `fsync` flushes file data and associated metadata, while a containing directory entry requires a separate directory sync. Runnel's atomic-write sequence explicitly performs both file and parent-directory syncs.

The counters now show how application submissions divide among full Raft-log rewrites, journal appends, and snapshots for this workload. No block-device or per-node cgroup I/O totals were collected, so the relationship between submitted bytes and filesystem/device writes remains unknown. This diagnostic also does not establish a supported clustered-cost bound; that still needs controlled repeated retained-log and purge-cycle measurements, latency/resource/recovery evidence, and a stated workload envelope. Per-path application attribution is now implemented, while physical-I/O attribution remains conditional on demonstrable host support and need.

## Controlled counter-enabled/disabled diagnostic

The final comparison ran on 2026-10-06 under the exclusive benchmark lock. Three alternated pairs ran in this order: default-feature/counters, counters/default-feature, default-feature/counters. Each case used `raft_log_growth` with 256 measured 1-KiB records, `publish_batch` size 8, file observation every 8 publishes, a 30-second snapshot/purge wait, three native broker processes, and the same recovery poll/ack path. Setup offset 0 was outside the measured interval; every measured record returned its expected contiguous published offset. All six raw runner artifacts reported `status: complete`.

The build input was supplied baseline commit `4a1d7333c19165d6c5c820460278772fd7dba4a1` plus the same final uncommitted worktree source for both feature variants. The runner's `source.revision` field therefore identifies that baseline, not a commit containing this implementation. The SHA-256 of the 13 changed Cargo/Rust/Python source paths was `927241af2c32ab1efcd577387ecd7508ac5a6006bbaec35a0115f0ce5a5006db`, computed from sorted relative paths and exact file contents with NUL separators. The default and counter-enabled release binaries were `f7dd6b96196f23b159d1b053c0fd07c4c27f7c79850d956c537b7e78b1c1addf` and `c6179a1f8dd3509584ab33187bcf5414fa0efa0a4c0d78253e055538d8d98f52`, respectively. The only build difference was enabling `persistence-write-counters`. Both used `cargo build --locked --release -p runnel-server`, with `--features persistence-write-counters` added for the enabled binary; the runner used `--binary` and no `--build`.

The host was a 12th Gen Intel Core i9-12900H with 20 logical CPUs and Linux `7.0.0-34-generic`. Broker data, runner output, and broker logs were placed under `benchmark-results/td026-write-counters/` on ext4 backed by the local encrypted `/dev/mapper/luks-b76ba52f-297b-430b-8d5f-8a49fa4c257a` volume; `TMPDIR` pointed into this ext4-backed area rather than the host's `/tmp` tmpfs. Each case ran in a systemd user scope covering the client and all three brokers, limited to 200% CPU and 2 GiB memory. The benchmark's `resource_limits` metadata records those limits. No separate per-node cgroup or device statistics were collected.

| Pair order | Counter build | Run ID | Publish interval (s) | Throughput (msg/s) | p50 (ms) | p99 (ms) | p99.9 (ms) | File observer share |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | disabled | `20261006093957490213` | 26.433 | 9.685 | 848.96 | 907.89 | 908.94 | 0.62% |
| 1 | enabled | `20261006094025217168` | 20.375 | 12.564 | 625.43 | 696.12 | 706.69 | 0.91% |
| 2 | enabled | `20261006094046650779` | 19.722 | 12.980 | 605.77 | 666.04 | 667.86 | 0.80% |
| 2 | disabled | `20261006094107398227` | 19.719 | 12.983 | 604.77 | 669.59 | 677.76 | 0.86% |
| 3 | disabled | `20261006094128108649` | 19.789 | 12.936 | 608.63 | 668.71 | 672.45 | 0.90% |
| 3 | enabled | `20261006094148928113` | 19.806 | 12.926 | 610.18 | 662.67 | 666.13 | 0.89% |

Across three runs per variant, median interval time was 19.789 s disabled and 19.806 s enabled; median throughput was 12.936 and 12.926 msg/s. Median p50 was 608.63 and 610.18 ms, while median p99 was 669.59 and 666.04 ms. The first disabled run was an outlier at 26.433 s and 907.89 ms p99; the other two disabled runs took 19.719–19.789 s with p99 of 668.71–669.59 ms. Paired enabled/disabled throughput ratios were 1.297, 1.000, and 0.999; p99 ratios were 0.767, 0.995, and 0.991. With three pairs and this first-run deviation, the comparison is inconclusive for counter overhead or performance effect. File observation contributed 0.62%–0.91% of the measured interval.

Across the three enabled runs, each cluster submitted 137.59–138.04 MB through Raft-log rewrite `write_all`, 3.33–3.34 MB through journal append, and 42.28 MB through snapshot writes. Checkpoint writes were zero; journal compaction synced and renamed an empty compacted file with no `write_all` payload. All measured `write_all` calls succeeded, so offered and completed bytes matched and no accepted-prefix-unknown event occurred. For each run, Raft-log serialization produced 137.59–138.04 MB; cumulative time was 0.385–0.438 s for serialization, 0.112–0.132 s for `write_all`, 18.56–19.04 s for file `sync_all`, 0.358–0.436 s for rename, and 15.88–16.19 s for directory sync. Those stages had 1,560 calls, except file-sync/rename/directory-sync had 1,560–1,562. Journal serialization produced 3,329,742–3,338,410 bytes over 768–770 calls; `write_all` took 0.077–0.123 s over 1,536–1,540 calls, and `sync_data` took 9.00–9.22 s over 768–770 calls. Snapshot-state serialization produced 14,062,344 bytes over 24 calls, then stored-snapshot serialization produced and submitted 42,283,599 bytes over 24 calls. Snapshot-state serialization took 0.037–0.042 s; outer stored-snapshot serialization took 0.105–0.111 s, `write_all` 0.010–0.011 s, file sync 0.277–0.299 s, rename 0.010–0.013 s, and directory sync 0.323–0.330 s. The inner state and outer stored-snapshot serialization outputs are nested stages, not an additive payload total. Compaction's 24 empty-file sync/rename/directory-sync calls took 0.302–0.307 s, 0.006–0.008 s, and 0.319–0.330 s, respectively. These durations sum process-local operation times; they are not device time or a decomposition of cluster wall time.

For restarted follower node 3, the pre-stop Raft-rewrite submission was 45.72–45.99 MB across enabled runs. Each enabled artifact marks the process reset, records a zero-valued fresh-process baseline, and records a zero write delta through recovery poll/ack; no counter was subtracted across the restart. The final JSON artifacts and per-run logs remain locally, excluded from source control because their observation payloads are repetitive: `benchmark-results/td026-write-counters/ext4-artifacts/` (`pair1-off.json`, `pair1-on.json`, `pair2-on.json`, `pair2-off.json`, `pair3-off.json`, `pair3-on.json`, and `order.log`). Earlier pre-query-gate results are retained separately in `ext4-pre-query-gate-diagnostic/` and are superseded; they are not evidence for this final source.

This diagnostic estimates neither physical bytes written nor write amplification. `write_all` totals are application bytes accepted by successful writer calls; current file sizes remain separate footprint samples. It does not settle commit-cost growth across other log sizes, payloads, batch sizes, or snapshot/purge cycles.

## Disposition and next evidence

The isolated measurement answers the narrow storage question: the full-map rewrite's serialized output grows with retained encoded history. The live clustered scenario now confirms that the real benchmark can observe retained Raft-log growth, snapshot builds, purge-index advances, per-path file footprints, and restart replay/ack after compaction. Neither measurement establishes an accepted cost bound for clustered commits. The observed peaks are sampled under one workload and cannot establish the maximum retained-log length.

Keep [TD-026](../tech-debt.md#td-026-raft-log-persistence-rewrites-retained-entries) open. The bounded opt-in application-counter slice and one controlled enabled/disabled diagnostic are complete; that comparison is inconclusive for instrumentation overhead and does not establish the supported clustered-cost bound. Next evidence should vary retained-history size, payload, batch size, and snapshot/purge cycle under controlled repeated runs. Add cgroup or device-level totals only where separate attribution is demonstrated to be interpretable. Do not infer a product limit or redesign storage from this diagnostic.
