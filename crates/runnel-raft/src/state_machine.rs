use std::collections::BTreeMap;

use openraft::{BasicNode, LogId, StoredMembership};
use runnel_engine::{Offset, ReplayMessage};
use serde::{Deserialize, Serialize};

use super::NodeId;
use super::delivery;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    CreateStream {
        stream: String,
        #[serde(default)]
        stream_id: Option<String>,
        #[serde(default)]
        group_id: Option<String>,
    },
    InitializeDataStream {
        stream: String,
        stream_id: String,
        group_id: String,
    },
    ActivateStream {
        stream: String,
    },
    Publish {
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        published_at_ms: u64,
        #[serde(default)]
        request_id: Option<String>,
    },
    Ack {
        stream: String,
        consumer: String,
        offset: Offset,
    },
    Replay {
        stream: String,
        consumer: String,
        offset: Offset,
    },
    PollGroup {
        stream: String,
        consumer: String,
        member: String,
        now_ms: u64,
        lease_deadline_ms: u64,
        #[serde(default)]
        max_delivery_attempts: Option<u32>,
    },
    AckGroup {
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
        now_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommandResponse {
    StreamCreated {
        created: bool,
    },
    DataStreamInitialized {
        initialized: bool,
    },
    StreamActivated {
        activated: bool,
    },
    Published {
        offset: Offset,
    },
    Acknowledged,
    AlreadyAcknowledged,
    OutOfOrderAck {
        expected: Offset,
        received: Offset,
    },
    GroupPoll {
        result: runnel_engine::PollResult,
    },
    Replay {
        result: ReplayMessage,
    },
    HistoryUnavailable {
        requested_offset: Offset,
        earliest_offset: Offset,
        next_offset: Offset,
    },
    GroupAcknowledged,
    GroupAlreadyAcknowledged,
    GroupAckNotInFlight {
        consumer: String,
        offset: Offset,
    },
    GroupStaleDelivery {
        consumer: String,
        offset: Offset,
    },
    StreamNotFound,
    Noop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredMessage {
    pub(super) key: Option<String>,
    pub(super) payload: Vec<u8>,
    pub(super) published_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum StreamLifecycle {
    Creating,
    #[default]
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamMetadata {
    pub stream_id: String,
    pub group_id: String,
    pub lifecycle: StreamLifecycle,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct StreamState {
    pub(super) stream_id: String,
    pub(super) group_id: String,
    pub(super) lifecycle: StreamLifecycle,
    pub(super) messages: Vec<StoredMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum GroupKind {
    #[default]
    Combined,
    Metadata,
    Data {
        stream: String,
        stream_id: String,
        group_id: String,
    },
}

impl StreamState {
    pub(super) fn active(stream_id: String, group_id: String) -> Self {
        Self {
            stream_id,
            group_id,
            lifecycle: StreamLifecycle::Active,
            messages: Vec::new(),
        }
    }

    pub(super) fn metadata(&self, stream: &str) -> StreamMetadata {
        let (stream_id, group_id) = stream_identity(stream);
        StreamMetadata {
            stream_id: if self.stream_id.is_empty() {
                stream_id
            } else {
                self.stream_id.clone()
            },
            group_id: if self.group_id.is_empty() {
                group_id
            } else {
                self.group_id.clone()
            },
            lifecycle: self.lifecycle.clone(),
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.lifecycle == StreamLifecycle::Active
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct SnapshotState {
    pub(super) streams: BTreeMap<String, StreamState>,
    pub(super) consumers: BTreeMap<(String, String), Offset>,
    #[serde(default)]
    pub(super) group_consumers: BTreeMap<(String, String), delivery::GroupConsumerState>,
    // The lease evaluator is a replicated, persisted floor of command
    // observations. It prevents a backward wall-clock step from moving
    // expiry backwards after recovery or leader changes.
    #[serde(default)]
    pub(super) lease_clock_ms: u64,
    pub(super) dedup: BTreeMap<String, BTreeMap<String, Offset>>,
    #[serde(default)]
    pub(super) redeliveries: u64,
    #[serde(default)]
    pub(super) dead_letters: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct StateMachineData {
    pub(super) last_applied_log: Option<LogId<NodeId>>,
    pub(super) last_membership: StoredMembership<NodeId, BasicNode>,
    pub(super) state: SnapshotState,
}

pub(super) fn apply_command(
    state: &mut SnapshotState,
    command: Command,
    kind: &GroupKind,
    log_id: LogId<NodeId>,
) -> CommandResponse {
    match command {
        Command::CreateStream {
            stream,
            stream_id,
            group_id,
        } => {
            if matches!(kind, GroupKind::Data { .. }) {
                return CommandResponse::Noop;
            }
            let (derived_stream_id, derived_group_id) = stream_identity(&stream);
            let lifecycle = if matches!(kind, GroupKind::Metadata) {
                StreamLifecycle::Creating
            } else {
                StreamLifecycle::Active
            };
            let created = if let std::collections::btree_map::Entry::Vacant(entry) =
                state.streams.entry(stream)
            {
                entry.insert(StreamState {
                    stream_id: stream_id.unwrap_or(derived_stream_id),
                    group_id: group_id.unwrap_or(derived_group_id),
                    lifecycle,
                    messages: Vec::new(),
                });
                true
            } else {
                false
            };
            CommandResponse::StreamCreated { created }
        }
        Command::InitializeDataStream {
            stream,
            stream_id,
            group_id,
        } => {
            let GroupKind::Data {
                stream: expected_stream,
                stream_id: expected_stream_id,
                group_id: expected_group_id,
            } = kind
            else {
                return CommandResponse::Noop;
            };
            if &stream != expected_stream
                || &stream_id != expected_stream_id
                || &group_id != expected_group_id
            {
                return CommandResponse::Noop;
            }
            let initialized = if let std::collections::btree_map::Entry::Vacant(entry) =
                state.streams.entry(stream)
            {
                entry.insert(StreamState::active(stream_id, group_id));
                true
            } else {
                false
            };
            CommandResponse::DataStreamInitialized { initialized }
        }
        Command::ActivateStream { stream } => {
            if matches!(kind, GroupKind::Data { .. }) {
                return CommandResponse::Noop;
            }
            let Some(stream_state) = state.streams.get_mut(&stream) else {
                return CommandResponse::StreamActivated { activated: false };
            };
            let activated = stream_state.lifecycle != StreamLifecycle::Active;
            stream_state.lifecycle = StreamLifecycle::Active;
            CommandResponse::StreamActivated { activated }
        }
        Command::Publish {
            stream,
            key,
            payload,
            published_at_ms,
            request_id,
        } => {
            if matches!(kind, GroupKind::Metadata) {
                return CommandResponse::StreamNotFound;
            }
            if let Some(request_id) = request_id.as_ref()
                && let Some(offset) = state
                    .dedup
                    .get(&stream)
                    .and_then(|requests| requests.get(request_id))
            {
                return CommandResponse::Published { offset: *offset };
            }
            let (stream_id, group_id) = stream_identity(&stream);
            let stream_state = state
                .streams
                .entry(stream.clone())
                .or_insert_with(|| StreamState::active(stream_id, group_id));
            if !stream_state.is_active() {
                return CommandResponse::StreamNotFound;
            }
            let offset = stream_state.messages.len() as Offset;
            stream_state.messages.push(StoredMessage {
                key,
                payload,
                published_at_ms,
            });
            if let Some(request_id) = request_id {
                state
                    .dedup
                    .entry(stream)
                    .or_default()
                    .insert(request_id, offset);
            }
            CommandResponse::Published { offset }
        }
        Command::Ack {
            stream,
            consumer,
            offset,
        } => {
            if matches!(kind, GroupKind::Metadata) {
                return CommandResponse::StreamNotFound;
            }
            let Some(stream_state) = state.streams.get(&stream) else {
                return CommandResponse::StreamNotFound;
            };
            if !stream_state.is_active() {
                return CommandResponse::StreamNotFound;
            }
            let key = (stream, consumer);
            let expected = state.consumers.get(&key).copied().unwrap_or_default();
            if offset < expected {
                return CommandResponse::AlreadyAcknowledged;
            }
            if offset > expected {
                return CommandResponse::OutOfOrderAck {
                    expected,
                    received: offset,
                };
            }
            state.consumers.insert(key, offset + 1);
            CommandResponse::Acknowledged
        }
        Command::Replay {
            stream,
            consumer: _,
            offset,
        } => apply_replay(state, &stream, offset, kind),
        Command::PollGroup {
            stream,
            consumer,
            member,
            now_ms,
            lease_deadline_ms,
            max_delivery_attempts,
        } => delivery::apply_group_poll(
            state,
            delivery::GroupPollRequest {
                stream,
                consumer,
                member,
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts,
            },
            log_id,
            kind,
        ),
        Command::AckGroup {
            stream,
            consumer,
            member,
            offset,
            delivery_token,
            now_ms,
        } => delivery::apply_group_ack(
            state,
            delivery::GroupAckRequest {
                stream,
                consumer,
                member,
                offset,
                delivery_token,
                now_ms,
            },
            kind,
        ),
    }
}

fn apply_replay(
    state: &SnapshotState,
    stream: &str,
    offset: Offset,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let Some(stream_state) = state.streams.get(stream) else {
        return CommandResponse::StreamNotFound;
    };
    if !stream_state.is_active() {
        return CommandResponse::StreamNotFound;
    }

    let next_offset = stream_state.messages.len() as Offset;
    let Some(message) = stream_state.messages.get(offset as usize) else {
        return CommandResponse::HistoryUnavailable {
            requested_offset: offset,
            earliest_offset: 0,
            next_offset,
        };
    };
    CommandResponse::Replay {
        result: ReplayMessage {
            stream: stream.to_owned(),
            offset,
            key: message.key.clone(),
            payload: message.payload.clone(),
            published_at_ms: message.published_at_ms,
        },
    }
}

pub(super) fn stream_identity(stream: &str) -> (String, String) {
    (format!("stream/{stream}"), format!("group/{stream}/data"))
}
