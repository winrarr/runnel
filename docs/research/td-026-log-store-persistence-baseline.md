# TD-026 isolated Raft log persistence baseline

Status: isolated persistence growth and initial live clustered snapshot/purge evidence; the clustered commit-cost bound remains open

Measured: isolated persistence baseline on 2026-09-28; live clustered samples on 2026-09-29

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

## Write-attribution methods and next measurement slice

The 2026-10-06 repeated matrix adds payload and requested batch-size coverage, but the sampled file lengths still cannot tell how many bytes each persistence path submitted or how many block writes reached storage. Observer I/O is also a material part of these intervals: the tracked matrix summary reports 5.3%–35.5%, while a separate final diagnostic run at `f14f79e` reports 3.6%–35.6% in its local artifact. These are results from distinct runs and are observer-cost diagnostics, not publish-cost evidence. The methods below separate three signals that must not be conflated: bytes submitted by Runnel, current file footprint, and I/O accounted below the filesystem.

The relevant write sites are distinct. `LogStore::persist` serializes the complete retained log map and sends it through an atomic temporary-file write, file sync, rename, and parent-directory sync. State-machine apply appends each journal record as a length-prefixed JSON frame and calls `sync_data`. Snapshot and checkpoint persistence use atomic rewrites; snapshot building also compacts the journal by reading retained entries, writing a temporary journal, syncing it, renaming it, and syncing the directory. These paths are in `crates/runnel-raft/src/log_store.rs`, `crates/runnel-raft/src/state_machine_journal.rs`, `crates/runnel-raft/src/state_machine_store.rs`, and `crates/runnel-raft/src/lib.rs`.

| Method | What it measures and attributes | Limits and cost | Fit for Runnel |
| --- | --- | --- | --- |
| Fixed application counters at persistence call sites | Per-role bytes passed to the journal frame writer or atomic-write helper, operation counts, and elapsed time in write, sync, rename, and directory-sync stages. The caller already knows whether the target is the Raft log, state journal, checkpoint, or snapshot. | Counts application submissions, not filesystem metadata, delayed writeback, or device writes. With `write_all`, input length is an exact completed submission only when the call succeeds; a failed call can have written a prefix. Aggregated fixed counters avoid per-operation output, but their overhead still needs a counter-on/off check. [`Write::write_all`](https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all) retries partial writes until the buffer is accepted or an error occurs. | Best first slice: narrow, path-aware, portable across the broker's supported platforms, and directly distinguishes the four persistence roles. Use a fixed role enum and aggregate counters rather than stream/group labels or event logs. |
| Syscall tracing (`strace` or `perf trace`) | Returned byte counts and call duration for `write*`, `pwrite*`, `fsync`, `fdatasync`, and rename syscalls. With open/close and descriptor tracking, it can validate application counters and expose extra writes made by the process. [`strace`](https://man7.org/linux/man-pages/man1/strace.1.html) can filter by syscall, PID, file descriptor, and path; [`perf trace`](https://man7.org/linux/man-pages/man1/perf-trace.1.html) supports syscall summaries and cgroup selection. | Captures the process-to-kernel boundary, not subsequent asynchronous writeback or device traffic. A pathname may become a temporary name and then be renamed; tracing must maintain descriptor/path state and include all relevant syscalls. `strace` uses [`ptrace`](https://man7.org/linux/man-pages/man2/ptrace.2.html); its own manual provides a syscall-overhead adjustment, so detailed tracing can perturb the workload and trace timings need calibration. `perf trace` depends on perf-event access policy and kernel/tool support. Use it as a bounded validation run, not as the primary timed result. | Useful for checking that path counters cover actual syscalls, especially temp-file, sync, rename, and directory operations. It will include unrelated process writes unless filtered and attributed carefully. |
| Kernel VFS/filesystem tracing or eBPF aggregation | Kernel events can count or time writes below the syscall layer and can be filtered by process/cgroup. The kernel [event tracing](https://docs.kernel.org/trace/events.html) interface exposes available tracepoints through tracefs, and tools such as ftrace and perf can consume them. | Tracepoint availability and fields vary by kernel/filesystem; VFS function probes can be less stable than tracepoints. Path resolution across file descriptors, temporary files, rename, and writeback complicates per-file grouping. Event capture, buffers, and access controls add operational overhead; verify privileges and lost-event counters. The kernel [perf access policy](https://docs.kernel.org/admin-guide/perf-security.html) may restrict tracing to privileged users/capabilities. It still does not prove which application operation caused a later merged or delayed block request. | Defer until app counters and a cheaper system boundary disagree or leave a specific question unanswered. It is a diagnostic escalation, not needed to establish submitted bytes by path. |
| File footprint (`st_size`, optionally allocated blocks) | Start/end logical length for each persisted path; on Linux, `st_blocks` can add allocated-block footprint, as described by the Linux [`stat`](https://man7.org/linux/man-pages/man2/stat.2.html) interface. This extends the current `file_bytes` samples with a separate footprint field. | A footprint is a point-in-time property, not cumulative writes. Rewriting an equal-sized file can produce zero net change; atomic rename replaces the old inode, and the temporary file may be gone by the next sample. `st_size` is logical length, while allocated blocks and filesystem accounting have different semantics. | Keep as a low-frequency side measurement (start/end or snapshot boundaries), not an attribution substitute. Remove repeated JSON file reads from timed samples where possible. |
| cgroup `io.stat` and block-device statistics | cgroup v2 [`io.stat`](https://docs.kernel.org/admin-guide/cgroup-v2.html#io-interface-files) provides per-device read/write bytes and I/O counts for a cgroup. [`/sys/block/<dev>/stat`](https://docs.kernel.org/block/stat.html) reports completed requests and sectors; its sector counters use 512-byte sectors. A dedicated cgroup can associate block I/O with a Runnel process group; device counters provide an aggregate cross-check. | cgroup counters aggregate all cgroup I/O, not paths, and buffered-writeback attribution requires filesystem support. The kernel documents support for ext2, ext4, btrfs, f2fs, and xfs; on other filesystems writeback can be charged to the root cgroup, and concurrent writers to a shared inode can be misattributed. Device counters are system-wide and include unrelated activity, filesystem metadata, and journal writes. Layered devices such as encrypted volumes need a declared accounting layer; summing both mapped and backing-device counters can count the same logical I/O twice. Neither signal is NAND/media write amplification. | Use `io.stat` per node only when cgroup v2, the I/O controller, filesystem writeback attribution, and process isolation are confirmed. Otherwise record block-device deltas only on an otherwise idle host and label them system-wide. Treat these as filesystem/device-boundary totals, never per-path totals. |

Runnel already has optional [`instrumentation`](../../crates/runnel-raft/Cargo.toml) `StageTimer` spans for `raft.log_append`, `raft.state_persist`, and `raft.state_checkpoint`. They cover broad methods, but do not report byte/operation counts or split serialization, write, sync, and rename stages. The timing guards in [`runnel-engine`](../../crates/runnel-engine/src/lib.rs) emit a trace event per method call, and the benchmark [profiling guidance](../../scripts/benchmarks/README.md#profiling) warns that these timings should not be used for uncontaminated performance comparisons. Keep those timers as coarse context; the next slice should use bounded aggregate counters rather than adding another per-operation trace stream.

Application-side counters are the recommended next measurement slice. Keep them opt-in to the benchmark/instrumented build, aggregate by a small fixed path-role and operation set plus node, and emit only at workload boundaries. Useful operation labels are Raft append/vote/commit-index/truncate/purge, journal append/compact, and checkpoint/snapshot write; do not use stream/group IDs. Time serialization separately from file writes. Count the buffer bytes offered to the journal append/compaction writers and atomic-write helpers, and count successful and failed operations separately. A successful `write_all` establishes that the complete input buffer was accepted by the writer; after an error, the accepted prefix length is unknown at this boundary, so mark it unknown rather than reporting the offered length as bytes written. Record write, file-sync, rename, and directory-sync call counts and cumulative durations. Do not create per-commit trace records in the timed path. Keep file footprint as a distinct before/after field. For atomic writes, report the serialized payload bytes offered to the temporary file separately from sync and rename work; the rename changes the active file without itself representing payload bytes.

Then run the existing bounded clustered workload with counters enabled and disabled on the same otherwise idle host, using identical workload, resources, storage, and durability settings. Report the instrumentation delta as an evidence limit, and do not infer cost from a run whose observer/counter overhead is material. Capture `io.stat` around each node only if the benchmark can place each broker in a separate cgroup and the filesystem/controller support is confirmed; also record the exact major:minor device and whether it is the filesystem-facing mapped device or a backing device. If this is unavailable, a before/after `/sys/block/<dev>/stat` read on an idle host is a separately labeled whole-device cross-check, not path attribution.

Use syscall tracing only for a short calibration run that checks the path counters and operation coverage, and keep it out of the headline timed comparison. Correlate traces to workload phases with start/stop markers; verify the trace and application timestamp clocks before joining their timestamps. Even with aligned clocks, interval totals do not establish which writeback or block completion was caused by a specific application call. [`fsync`/`fdatasync`](https://man7.org/linux/man-pages/man2/fsync.2.html) boundaries are particularly useful to retain: Linux documents that `fsync` flushes file data and associated metadata, while a containing directory entry requires a separate directory sync. Runnel's atomic-write sequence explicitly performs both file and parent-directory syncs.

This next slice should answer whether observed application submissions are concentrated in full Raft-log rewrites versus journal/snapshot work, and whether total block writes move with those submissions on the measured filesystem. It will not establish a supported clustered-cost bound by itself: that still needs controlled repeated retained-log and purge-cycle measurements, latency/resource/recovery evidence, and a stated workload envelope. Per-path write attribution is already an explicit remaining TD-026/backlog outcome, so this recommendation does not warrant a separate tracker item or change the existing retirement condition.

## Disposition and next evidence

The isolated measurement answers the narrow storage question: the full-map rewrite's serialized output grows with retained encoded history. The live clustered scenario now confirms that the real benchmark can observe retained Raft-log growth, snapshot builds, purge-index advances, per-path file footprints, and restart replay/ack after compaction. Neither measurement establishes an accepted cost bound for clustered commits. The observed peaks are sampled under one workload and cannot establish the maximum retained-log length.

Keep [TD-026](../tech-debt.md#td-026-raft-log-persistence-rewrites-retained-entries) open. Next evidence should implement the bounded opt-in application-counter slice above, then repeat live runs under controlled observation overhead while varying message count, payload, and append batch size through multiple snapshot/purge cycles. Add cgroup or device-level totals only where the host supports interpretable attribution. Do not infer a product limit or redesign storage from these initial observations.
