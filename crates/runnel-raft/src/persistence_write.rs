use std::io::{self, Write};

use serde::Serialize;

#[cfg(feature = "persistence-write-counters")]
const PERSISTENCE_WRITE_ROLE_COUNT: usize = 7;
#[cfg(feature = "persistence-write-counters")]
const PERSISTENCE_WRITE_OPERATION_COUNT: usize = 7;

#[cfg_attr(not(feature = "persistence-write-counters"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum PersistenceWriteRole {
    RaftLogRewrite,
    StateMachineJournalAppend,
    StateMachineJournalCompaction,
    StateMachineCheckpoint,
    StateMachineSnapshot,
    RaftLogSegmentAppend,
    RaftLogControlState,
}

impl PersistenceWriteRole {
    #[cfg(feature = "persistence-write-counters")]
    pub const ALL: [Self; PERSISTENCE_WRITE_ROLE_COUNT] = [
        Self::RaftLogRewrite,
        Self::StateMachineJournalAppend,
        Self::StateMachineJournalCompaction,
        Self::StateMachineCheckpoint,
        Self::StateMachineSnapshot,
        Self::RaftLogSegmentAppend,
        Self::RaftLogControlState,
    ];

    #[cfg(feature = "persistence-write-counters")]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RaftLogRewrite => "raft_log_rewrite",
            Self::StateMachineJournalAppend => "state_machine_journal_append",
            Self::StateMachineJournalCompaction => "state_machine_journal_compaction",
            Self::StateMachineCheckpoint => "state_machine_checkpoint",
            Self::StateMachineSnapshot => "state_machine_snapshot",
            Self::RaftLogSegmentAppend => "raft_log_segment_append",
            Self::RaftLogControlState => "raft_log_control_state",
        }
    }
}

#[cfg_attr(not(feature = "persistence-write-counters"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum PersistenceWriteOperation {
    Serialize,
    SnapshotStateSerialize,
    WriteAll,
    SyncData,
    SyncAll,
    Rename,
    DirectorySync,
}

impl PersistenceWriteOperation {
    #[cfg(feature = "persistence-write-counters")]
    pub const ALL: [Self; PERSISTENCE_WRITE_OPERATION_COUNT] = [
        Self::Serialize,
        Self::SnapshotStateSerialize,
        Self::WriteAll,
        Self::SyncData,
        Self::SyncAll,
        Self::Rename,
        Self::DirectorySync,
    ];

    #[cfg(feature = "persistence-write-counters")]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Serialize => "serialize",
            Self::SnapshotStateSerialize => "snapshot_state_serialize",
            Self::WriteAll => "write_all",
            Self::SyncData => "sync_data",
            Self::SyncAll => "sync_all",
            Self::Rename => "rename",
            Self::DirectorySync => "directory_sync",
        }
    }
}

#[cfg(feature = "persistence-write-counters")]
mod enabled {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    pub struct PersistenceWriteCounterSnapshot {
        pub attempts: u64,
        pub successes: u64,
        pub failures: u64,
        pub elapsed_nanoseconds: u64,
        pub write_bytes_offered: u64,
        pub write_bytes_completed: u64,
        pub write_accepted_prefix_unknown: u64,
        pub serialization_output_bytes: u64,
    }

    struct CounterCell {
        attempts: AtomicU64,
        successes: AtomicU64,
        failures: AtomicU64,
        elapsed_nanoseconds: AtomicU64,
        write_bytes_offered: AtomicU64,
        write_bytes_completed: AtomicU64,
        write_accepted_prefix_unknown: AtomicU64,
        serialization_output_bytes: AtomicU64,
    }

    impl CounterCell {
        const fn new() -> Self {
            Self {
                attempts: AtomicU64::new(0),
                successes: AtomicU64::new(0),
                failures: AtomicU64::new(0),
                elapsed_nanoseconds: AtomicU64::new(0),
                write_bytes_offered: AtomicU64::new(0),
                write_bytes_completed: AtomicU64::new(0),
                write_accepted_prefix_unknown: AtomicU64::new(0),
                serialization_output_bytes: AtomicU64::new(0),
            }
        }

        fn record(
            &self,
            operation: PersistenceWriteOperation,
            succeeded: bool,
            elapsed_nanoseconds: u64,
            offered_bytes: u64,
            serialized_bytes: u64,
        ) {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            if succeeded {
                self.successes.fetch_add(1, Ordering::Relaxed);
            } else {
                self.failures.fetch_add(1, Ordering::Relaxed);
            }
            self.elapsed_nanoseconds
                .fetch_add(elapsed_nanoseconds, Ordering::Relaxed);

            match operation {
                PersistenceWriteOperation::WriteAll => {
                    self.write_bytes_offered
                        .fetch_add(offered_bytes, Ordering::Relaxed);
                    if succeeded {
                        self.write_bytes_completed
                            .fetch_add(offered_bytes, Ordering::Relaxed);
                    } else {
                        // `write_all` does not report how much of the offered slice was
                        // accepted before an error, so keep the unknown prefix explicit.
                        self.write_accepted_prefix_unknown
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                PersistenceWriteOperation::Serialize
                | PersistenceWriteOperation::SnapshotStateSerialize => {
                    self.serialization_output_bytes
                        .fetch_add(serialized_bytes, Ordering::Relaxed);
                }
                _ => {}
            }
        }

        fn snapshot(&self) -> PersistenceWriteCounterSnapshot {
            PersistenceWriteCounterSnapshot {
                attempts: self.attempts.load(Ordering::Relaxed),
                successes: self.successes.load(Ordering::Relaxed),
                failures: self.failures.load(Ordering::Relaxed),
                elapsed_nanoseconds: self.elapsed_nanoseconds.load(Ordering::Relaxed),
                write_bytes_offered: self.write_bytes_offered.load(Ordering::Relaxed),
                write_bytes_completed: self.write_bytes_completed.load(Ordering::Relaxed),
                write_accepted_prefix_unknown: self
                    .write_accepted_prefix_unknown
                    .load(Ordering::Relaxed),
                serialization_output_bytes: self.serialization_output_bytes.load(Ordering::Relaxed),
            }
        }
    }

    struct CounterSet {
        cells: [CounterCell; PERSISTENCE_WRITE_ROLE_COUNT * PERSISTENCE_WRITE_OPERATION_COUNT],
    }

    impl CounterSet {
        const fn new() -> Self {
            Self {
                cells: [const { CounterCell::new() };
                    PERSISTENCE_WRITE_ROLE_COUNT * PERSISTENCE_WRITE_OPERATION_COUNT],
            }
        }

        fn cell(
            &self,
            role: PersistenceWriteRole,
            operation: PersistenceWriteOperation,
        ) -> &CounterCell {
            &self.cells[role as usize * PERSISTENCE_WRITE_OPERATION_COUNT + operation as usize]
        }

        fn record(
            &self,
            role: PersistenceWriteRole,
            operation: PersistenceWriteOperation,
            succeeded: bool,
            elapsed_nanoseconds: u64,
            offered_bytes: u64,
            serialized_bytes: u64,
        ) {
            self.cell(role, operation).record(
                operation,
                succeeded,
                elapsed_nanoseconds,
                offered_bytes,
                serialized_bytes,
            );
        }

        fn snapshot(
            &self,
        ) -> [[PersistenceWriteCounterSnapshot; PERSISTENCE_WRITE_OPERATION_COUNT];
            PERSISTENCE_WRITE_ROLE_COUNT] {
            std::array::from_fn(|role_index| {
                std::array::from_fn(|operation_index| {
                    self.cells[role_index * PERSISTENCE_WRITE_OPERATION_COUNT + operation_index]
                        .snapshot()
                })
            })
        }
    }

    static COUNTERS: CounterSet = CounterSet::new();

    #[derive(Debug, Clone)]
    pub struct PersistenceWriteMetricsSnapshot {
        cells: [[PersistenceWriteCounterSnapshot; PERSISTENCE_WRITE_OPERATION_COUNT];
            PERSISTENCE_WRITE_ROLE_COUNT],
    }

    impl PersistenceWriteMetricsSnapshot {
        pub fn counter(
            &self,
            role: PersistenceWriteRole,
            operation: PersistenceWriteOperation,
        ) -> PersistenceWriteCounterSnapshot {
            self.cells[role as usize][operation as usize]
        }
    }

    pub fn snapshot() -> PersistenceWriteMetricsSnapshot {
        PersistenceWriteMetricsSnapshot {
            cells: COUNTERS.snapshot(),
        }
    }

    pub fn serialize_json<T: Serialize>(
        role: PersistenceWriteRole,
        operation: PersistenceWriteOperation,
        value: &T,
    ) -> Result<Vec<u8>, serde_json::Error> {
        serialize_json_with_counters(value, role, operation, &COUNTERS)
    }

    fn serialize_json_with_counters<T: Serialize>(
        value: &T,
        role: PersistenceWriteRole,
        operation: PersistenceWriteOperation,
        counters: &CounterSet,
    ) -> Result<Vec<u8>, serde_json::Error> {
        let started = std::time::Instant::now();
        let result = serde_json::to_vec(value);
        let elapsed_nanoseconds = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let serialized_bytes = result.as_ref().map_or(0, |bytes| bytes.len() as u64);
        counters.record(
            role,
            operation,
            result.is_ok(),
            elapsed_nanoseconds,
            0,
            serialized_bytes,
        );
        result
    }

    pub fn write_all<W: Write>(
        writer: &mut W,
        role: PersistenceWriteRole,
        bytes: &[u8],
    ) -> io::Result<()> {
        measure_io(
            role,
            PersistenceWriteOperation::WriteAll,
            bytes.len() as u64,
            || writer.write_all(bytes),
        )
    }

    pub fn measure_io<T>(
        role: PersistenceWriteRole,
        operation: PersistenceWriteOperation,
        offered_bytes: u64,
        operation_fn: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let started = std::time::Instant::now();
        let result = operation_fn();
        let elapsed_nanoseconds = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        COUNTERS.record(
            role,
            operation,
            result.is_ok(),
            elapsed_nanoseconds,
            offered_bytes,
            0,
        );
        result
    }

    #[cfg(test)]
    fn write_all_with_counters<W: Write>(
        writer: &mut W,
        role: PersistenceWriteRole,
        bytes: &[u8],
        counters: &CounterSet,
    ) -> io::Result<()> {
        let started = std::time::Instant::now();
        let result = writer.write_all(bytes);
        counters.record(
            role,
            PersistenceWriteOperation::WriteAll,
            result.is_ok(),
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            bytes.len() as u64,
            0,
        );
        result
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io;

        struct PartialThenFail {
            accepted: usize,
        }

        impl Write for PartialThenFail {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.accepted > 0 {
                    return Err(io::Error::other("injected failure after partial write"));
                }
                self.accepted = bytes.len().min(2);
                Ok(self.accepted)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        #[test]
        fn role_and_operation_classification_is_fixed_and_unique() {
            assert_eq!(PersistenceWriteRole::ALL.len(), 7);
            assert_eq!(PersistenceWriteOperation::ALL.len(), 7);
            let roles = PersistenceWriteRole::ALL.map(PersistenceWriteRole::as_str);
            let operations = PersistenceWriteOperation::ALL.map(PersistenceWriteOperation::as_str);
            for (index, role) in roles.iter().enumerate() {
                assert!(!roles[index + 1..].contains(role));
            }
            for (index, operation) in operations.iter().enumerate() {
                assert!(!operations[index + 1..].contains(operation));
            }
        }

        #[test]
        fn successful_write_all_aggregates_offered_and_completed_bytes() {
            let counters = CounterSet::new();
            let mut output = Vec::new();
            write_all_with_counters(
                &mut output,
                PersistenceWriteRole::StateMachineJournalAppend,
                b"record",
                &counters,
            )
            .unwrap();
            write_all_with_counters(
                &mut output,
                PersistenceWriteRole::StateMachineJournalAppend,
                b"tail",
                &counters,
            )
            .unwrap();

            let counter = counters
                .cell(
                    PersistenceWriteRole::StateMachineJournalAppend,
                    PersistenceWriteOperation::WriteAll,
                )
                .snapshot();
            assert_eq!(counter.attempts, 2);
            assert_eq!(counter.successes, 2);
            assert_eq!(counter.failures, 0);
            assert!(counter.elapsed_nanoseconds > 0);
            assert_eq!(counter.write_bytes_offered, 10);
            assert_eq!(counter.write_bytes_completed, 10);
            assert_eq!(counter.write_accepted_prefix_unknown, 0);
            assert_eq!(output, b"recordtail");
        }

        #[test]
        fn failed_write_all_marks_accepted_prefix_unknown_without_crediting_bytes() {
            let counters = CounterSet::new();
            let mut writer = PartialThenFail { accepted: 0 };
            let error = write_all_with_counters(
                &mut writer,
                PersistenceWriteRole::RaftLogRewrite,
                b"four-bytes",
                &counters,
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Other);

            let counter = counters
                .cell(
                    PersistenceWriteRole::RaftLogRewrite,
                    PersistenceWriteOperation::WriteAll,
                )
                .snapshot();
            assert_eq!(writer.accepted, 2);
            assert_eq!(counter.attempts, 1);
            assert_eq!(counter.successes, 0);
            assert_eq!(counter.failures, 1);
            assert!(counter.elapsed_nanoseconds > 0);
            assert_eq!(counter.write_bytes_offered, 10);
            assert_eq!(counter.write_bytes_completed, 0);
            assert_eq!(counter.write_accepted_prefix_unknown, 1);
        }

        #[test]
        fn serialization_output_bytes_are_separate_from_file_write_bytes() {
            let counters = CounterSet::new();
            let value = vec![1_u8, 2, 3];
            let encoded = serialize_json_with_counters(
                &value,
                PersistenceWriteRole::StateMachineSnapshot,
                PersistenceWriteOperation::SnapshotStateSerialize,
                &counters,
            )
            .unwrap();
            let counter = counters
                .cell(
                    PersistenceWriteRole::StateMachineSnapshot,
                    PersistenceWriteOperation::SnapshotStateSerialize,
                )
                .snapshot();
            assert_eq!(counter.serialization_output_bytes, encoded.len() as u64);
            assert_eq!(counter.successes, 1);
            assert_eq!(counter.failures, 0);
            assert!(counter.elapsed_nanoseconds > 0);
            assert_eq!(counter.write_bytes_offered, 0);
            assert_eq!(counter.write_bytes_completed, 0);
        }
    }
}

#[cfg(feature = "persistence-write-counters")]
pub use enabled::{
    PersistenceWriteCounterSnapshot, PersistenceWriteMetricsSnapshot,
    snapshot as persistence_write_metrics_snapshot,
};

#[cfg(feature = "persistence-write-counters")]
pub(crate) use enabled::{measure_io, serialize_json, write_all};

#[cfg(not(feature = "persistence-write-counters"))]
#[inline(always)]
pub(crate) fn serialize_json<T: Serialize>(
    _role: PersistenceWriteRole,
    _operation: PersistenceWriteOperation,
    value: &T,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(value)
}

#[cfg(not(feature = "persistence-write-counters"))]
#[inline(always)]
pub(crate) fn write_all<W: Write>(
    writer: &mut W,
    _role: PersistenceWriteRole,
    bytes: &[u8],
) -> io::Result<()> {
    writer.write_all(bytes)
}

#[cfg(not(feature = "persistence-write-counters"))]
#[inline(always)]
pub(crate) fn measure_io<T>(
    _role: PersistenceWriteRole,
    _operation: PersistenceWriteOperation,
    _offered_bytes: u64,
    operation_fn: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    operation_fn()
}
