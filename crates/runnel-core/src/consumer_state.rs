use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{BrokerError, Offset};
use serde::{Deserialize, Serialize};

pub(super) const MAX_CONSUMER_STATE_JOURNAL_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ConsumerState {
    pub(super) stream: String,
    pub(super) consumer: String,
    pub(super) committed_offset: Offset,
    #[serde(default)]
    pub(super) acknowledged_offsets: BTreeSet<Offset>,
    #[serde(default)]
    pub(super) delivery_attempts: BTreeMap<Offset, u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum ConsumerStateEvent {
    DeliveryAttempt { offset: Offset, attempt: u32 },
    Acknowledge { offset: Offset },
}

impl ConsumerState {
    pub(super) fn acknowledge(&mut self, offset: Offset) {
        if offset < self.committed_offset || self.acknowledged_offsets.contains(&offset) {
            return;
        }
        self.delivery_attempts.remove(&offset);
        if offset == self.committed_offset {
            self.committed_offset += 1;
            while self.acknowledged_offsets.remove(&self.committed_offset) {
                self.delivery_attempts.remove(&self.committed_offset);
                self.committed_offset += 1;
            }
        } else {
            self.acknowledged_offsets.insert(offset);
        }
    }

    fn apply_event(&mut self, event: ConsumerStateEvent) -> Result<(), BrokerError> {
        match event {
            ConsumerStateEvent::DeliveryAttempt { offset, attempt } => {
                if attempt == 0 {
                    return Err(BrokerError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "consumer delivery attempt must be greater than zero",
                    )));
                }
                if offset >= self.committed_offset && !self.acknowledged_offsets.contains(&offset) {
                    self.delivery_attempts
                        .entry(offset)
                        .and_modify(|current| *current = (*current).max(attempt))
                        .or_insert(attempt);
                }
            }
            ConsumerStateEvent::Acknowledge { offset } => self.acknowledge(offset),
        }
        Ok(())
    }
}

pub(super) fn load_consumer_state(
    root: &Path,
    stream: &str,
    consumer: &str,
) -> Result<ConsumerState, BrokerError> {
    let path = consumer_state_path(root, stream, consumer);
    let mut state = if path.exists() {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes)?
    } else {
        ConsumerState {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
            committed_offset: 0,
            acknowledged_offsets: BTreeSet::new(),
            delivery_attempts: BTreeMap::new(),
        }
    };
    replay_consumer_state_journal(root, stream, consumer, &mut state)?;
    Ok(state)
}

pub(super) fn persist_consumer_event(
    root: &Path,
    stream: &str,
    consumer: &str,
    current_state: &ConsumerState,
    event: ConsumerStateEvent,
) -> Result<(), BrokerError> {
    #[cfg(feature = "instrumentation")]
    let _stage_timer = StageTimer::new("core.consumer_state_persist");
    let path = consumer_state_journal_path(root, stream, consumer);
    let parent = path.parent().ok_or_else(|| {
        BrokerError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "consumer state journal has no parent",
        ))
    })?;
    let mut encoded = serde_json::to_vec(&event)?;
    encoded.push(b'\n');

    let journal_len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    if journal_len.saturating_add(encoded.len() as u64) > MAX_CONSUMER_STATE_JOURNAL_BYTES {
        // The checkpoint is published before the journal is truncated. If a process stops
        // between those steps, replaying the old events is idempotent and cannot move progress
        // backwards or revive an acknowledged delivery attempt.
        let mut checkpoint = current_state.clone();
        checkpoint.stream = stream.to_owned();
        checkpoint.consumer = consumer.to_owned();
        persist_consumer_state(root, &checkpoint)?;
        truncate_consumer_state_journal(&path, 0)?;
    }

    fs::create_dir_all(parent)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    Ok(())
}

fn consumer_state_path(root: &Path, stream: &str, consumer: &str) -> PathBuf {
    root.join("consumers")
        .join(stream)
        .join(format!("{consumer}.json"))
}

fn consumer_state_journal_path(root: &Path, stream: &str, consumer: &str) -> PathBuf {
    // Keep the historical temporary path as the event journal so existing storage-failure
    // boundaries continue to exercise the same blocking point. Checkpoint compaction uses a
    // separate temporary path below and never replaces this append-only file.
    consumer_state_path(root, stream, consumer).with_extension("json.tmp")
}

fn replay_consumer_state_journal(
    root: &Path,
    stream: &str,
    consumer: &str,
    state: &mut ConsumerState,
) -> Result<(), BrokerError> {
    let path = consumer_state_journal_path(root, stream, consumer);
    if !path.exists() {
        return Ok(());
    }

    let bytes = fs::read(&path)?;
    if serde_json::from_slice::<ConsumerState>(&bytes).is_ok() {
        // A pre-journal process may have left a fully written checkpoint temporary behind. It
        // was never authoritative, so retain the old recovery behavior and ignore it.
        return Ok(());
    }
    if bytes.len() as u64 > MAX_CONSUMER_STATE_JOURNAL_BYTES {
        return Err(BrokerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "consumer state journal exceeds its configured bound",
        )));
    }
    let mut valid_length = 0;
    for (line_index, line) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
        if !line.ends_with(b"\n") {
            break;
        }
        let line = &line[..line.len() - 1];
        let event = match serde_json::from_slice::<ConsumerStateEvent>(line) {
            Ok(event) => event,
            Err(_) if line_index == 0 && serde_json::from_slice::<ConsumerState>(line).is_ok() => {
                valid_length += line.len() + 1;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        state.apply_event(event)?;
        valid_length += line.len() + 1;
    }

    if valid_length != bytes.len() {
        truncate_consumer_state_journal(&path, valid_length as u64)?;
    }
    Ok(())
}

fn truncate_consumer_state_journal(path: &Path, length: u64) -> Result<(), BrokerError> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(length)?;
    file.sync_all()?;
    Ok(())
}

fn persist_consumer_state(root: &Path, state: &ConsumerState) -> Result<(), BrokerError> {
    #[cfg(feature = "instrumentation")]
    let _stage_timer = StageTimer::new("core.consumer_state_persist");
    let path = consumer_state_path(root, &state.stream, &state.consumer);
    let parent = path.parent().ok_or_else(|| {
        BrokerError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "consumer state has no parent",
        ))
    })?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("checkpoint.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temporary)?;
    serde_json::to_writer(&mut file, state)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ConsumerState {
        ConsumerState {
            stream: "events".to_owned(),
            consumer: "worker".to_owned(),
            committed_offset: 0,
            acknowledged_offsets: BTreeSet::new(),
            delivery_attempts: BTreeMap::new(),
        }
    }

    #[test]
    fn acknowledgement_advances_through_out_of_order_offsets() {
        let mut state = state();
        state.delivery_attempts.insert(0, 2);
        state.delivery_attempts.insert(1, 1);

        state.acknowledge(1);
        assert_eq!(state.committed_offset, 0);
        assert_eq!(state.acknowledged_offsets, BTreeSet::from([1]));

        state.acknowledge(0);
        assert_eq!(state.committed_offset, 2);
        assert!(state.acknowledged_offsets.is_empty());
        assert!(state.delivery_attempts.is_empty());
    }

    #[test]
    fn delivery_attempt_replay_keeps_the_highest_attempt() {
        let mut state = state();
        state
            .apply_event(ConsumerStateEvent::DeliveryAttempt {
                offset: 0,
                attempt: 3,
            })
            .unwrap();
        state
            .apply_event(ConsumerStateEvent::DeliveryAttempt {
                offset: 0,
                attempt: 2,
            })
            .unwrap();
        assert_eq!(state.delivery_attempts.get(&0), Some(&3));

        let error = state.apply_event(ConsumerStateEvent::DeliveryAttempt {
            offset: 1,
            attempt: 0,
        });
        assert!(
            matches!(error, Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
        );
    }
}
