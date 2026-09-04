use std::collections::{BTreeMap, BTreeSet};

use runnel_engine::{Message, Offset, PollResult};
use serde::{Deserialize, Serialize};

use super::state_machine::{CommandResponse, GroupKind, SnapshotState, StoredMessage, StreamState};

const DEAD_LETTER_SUFFIX: &str = ".dead-letter";
const DEAD_LETTER_HASH_PREFIX: &str = "runnel.dead-letter.";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct GroupDelivery {
    pub(super) member: String,
    pub(super) key: Option<String>,
    pub(super) delivery_attempt: u32,
    pub(super) delivery_token: String,
    pub(super) deadline_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct GroupConsumerState {
    pub(super) committed_offset: Offset,
    #[serde(default)]
    pub(super) acknowledged_offsets: BTreeSet<Offset>,
    #[serde(default)]
    pub(super) delivery_attempts: BTreeMap<Offset, u32>,
    #[serde(default)]
    pub(super) in_flight: BTreeMap<Offset, GroupDelivery>,
}

pub(super) struct GroupPollRequest {
    pub(super) stream: String,
    pub(super) consumer: String,
    pub(super) member: String,
    pub(super) now_ms: u64,
    pub(super) lease_deadline_ms: u64,
    pub(super) max_delivery_attempts: Option<u32>,
}

pub(super) struct GroupAckRequest {
    pub(super) stream: String,
    pub(super) consumer: String,
    pub(super) member: String,
    pub(super) offset: Offset,
    pub(super) delivery_token: String,
    pub(super) now_ms: u64,
}

pub(super) fn apply_group_poll(
    state: &mut SnapshotState,
    request: GroupPollRequest,
    log_id: openraft::LogId<super::NodeId>,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let GroupPollRequest {
        stream,
        consumer,
        member,
        now_ms,
        lease_deadline_ms,
        max_delivery_attempts,
    } = request;
    if !state
        .streams
        .get(&stream)
        .is_some_and(StreamState::is_active)
    {
        return CommandResponse::StreamNotFound;
    }
    let now_ms = observe_lease_clock(state, now_ms);

    let consumer_key = (stream.clone(), consumer.clone());
    if !state.group_consumers.contains_key(&consumer_key) {
        let committed_offset = state
            .consumers
            .get(&consumer_key)
            .copied()
            .unwrap_or_default();
        state.group_consumers.insert(
            consumer_key.clone(),
            GroupConsumerState {
                committed_offset,
                ..GroupConsumerState::default()
            },
        );
    }
    let existing = {
        let consumer_state = state
            .group_consumers
            .entry(consumer_key.clone())
            .or_default();
        let expired = consumer_state
            .in_flight
            .iter()
            .filter_map(|(offset, delivery)| {
                lease_expired(delivery.deadline_ms, now_ms).then_some(*offset)
            })
            .collect::<Vec<_>>();
        for offset in expired {
            consumer_state.in_flight.remove(&offset);
        }
        consumer_state
            .in_flight
            .iter()
            .find(|(_, delivery)| delivery.member == member)
            .map(|(offset, delivery)| (*offset, delivery.clone()))
    };

    if let Some((offset, delivery)) = existing {
        let messages = &state
            .streams
            .get(&stream)
            .expect("stream was checked above")
            .messages;
        return group_poll_message(&stream, offset, messages, &delivery);
    }

    loop {
        let candidate = {
            let consumer_state = state
                .group_consumers
                .get(&consumer_key)
                .expect("group consumer state was initialized above");
            let stream_state = state
                .streams
                .get(&stream)
                .expect("stream was checked above");
            stream_state
                .messages
                .iter()
                .enumerate()
                .map(|(offset, message)| (offset as Offset, message))
                .filter(|(offset, _)| *offset >= consumer_state.committed_offset)
                .find(|(offset, message)| {
                    if consumer_state.acknowledged_offsets.contains(offset)
                        || consumer_state.in_flight.contains_key(offset)
                    {
                        return false;
                    }
                    message.key.as_ref().is_none_or(|key| {
                        !consumer_state
                            .in_flight
                            .values()
                            .any(|delivery| delivery.key.as_ref() == Some(key))
                    })
                })
                .map(|(offset, message)| (offset, message.key.clone()))
        };
        let Some((offset, key)) = candidate else {
            return CommandResponse::GroupPoll {
                result: PollResult::Empty,
            };
        };

        let attempts = state
            .group_consumers
            .get(&consumer_key)
            .expect("group consumer state was initialized above")
            .delivery_attempts
            .get(&offset)
            .copied()
            .unwrap_or_default();
        if max_delivery_attempts.is_some_and(|max| attempts >= max)
            && !is_dead_letter_stream(&stream)
        {
            let original = state
                .streams
                .get(&stream)
                .and_then(|stream_state| stream_state.messages.get(offset as usize))
                .cloned()
                .expect("candidate offset must refer to a stored message");
            let dead_letter_stream = dead_letter_stream_name(&stream);
            let (stream_id, group_id) = super::state_machine::stream_identity(&dead_letter_stream);
            state
                .streams
                .entry(dead_letter_stream)
                .or_insert_with(|| StreamState::active(stream_id, group_id))
                .messages
                .push(original);
            acknowledge_group_offset(
                state
                    .group_consumers
                    .get_mut(&consumer_key)
                    .expect("group consumer state was initialized above"),
                offset,
            );
            state.dead_letters = state.dead_letters.saturating_add(1);
            continue;
        }

        let (delivery_attempt, delivery) = {
            let consumer_state = state
                .group_consumers
                .get_mut(&consumer_key)
                .expect("group consumer state was initialized above");
            let delivery_attempt = consumer_state
                .delivery_attempts
                .entry(offset)
                .and_modify(|attempt| *attempt = attempt.saturating_add(1))
                .or_insert(1);
            let delivery = GroupDelivery {
                member: member.clone(),
                key,
                delivery_attempt: *delivery_attempt,
                delivery_token: format!("raft-{log_id}"),
                deadline_ms: lease_deadline_ms,
            };
            consumer_state.in_flight.insert(offset, delivery.clone());
            (*delivery_attempt, delivery)
        };
        if delivery_attempt > 1 {
            state.redeliveries = state.redeliveries.saturating_add(1);
        }
        let messages = &state
            .streams
            .get(&stream)
            .expect("stream was checked above")
            .messages;
        return group_poll_message(&stream, offset, messages, &delivery);
    }
}

fn group_poll_message(
    stream: &str,
    offset: Offset,
    messages: &[StoredMessage],
    delivery: &GroupDelivery,
) -> CommandResponse {
    let Some(message) = messages.get(offset as usize) else {
        return CommandResponse::StreamNotFound;
    };
    CommandResponse::GroupPoll {
        result: PollResult::Message(Message {
            stream: stream.to_owned(),
            offset,
            key: message.key.clone(),
            payload: message.payload.clone(),
            published_at_ms: message.published_at_ms,
            delivery_token: Some(delivery.delivery_token.clone()),
            delivery_attempt: Some(delivery.delivery_attempt),
        }),
    }
}

pub(super) fn apply_group_ack(
    state: &mut SnapshotState,
    request: GroupAckRequest,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let GroupAckRequest {
        stream,
        consumer,
        member,
        offset,
        delivery_token,
        now_ms,
    } = request;
    let Some(stream_state) = state.streams.get(&stream) else {
        return CommandResponse::StreamNotFound;
    };
    if !stream_state.is_active() {
        return CommandResponse::StreamNotFound;
    }

    let now_ms = observe_lease_clock(state, now_ms);
    let consumer_key = (stream, consumer.clone());
    let consumer_state = state.group_consumers.entry(consumer_key).or_default();
    let expired = consumer_state
        .in_flight
        .iter()
        .filter_map(|(offset, delivery)| {
            lease_expired(delivery.deadline_ms, now_ms).then_some(*offset)
        })
        .collect::<Vec<_>>();
    for expired_offset in expired {
        consumer_state.in_flight.remove(&expired_offset);
    }

    if offset < consumer_state.committed_offset
        || consumer_state.acknowledged_offsets.contains(&offset)
    {
        return CommandResponse::GroupAlreadyAcknowledged;
    }
    let Some(delivery) = consumer_state.in_flight.get(&offset) else {
        if delivery_token.is_empty()
            && member == consumer
            && offset == consumer_state.committed_offset
        {
            acknowledge_group_offset(consumer_state, offset);
            return CommandResponse::GroupAcknowledged;
        }
        return if consumer_state.delivery_attempts.contains_key(&offset) {
            CommandResponse::GroupStaleDelivery { consumer, offset }
        } else {
            CommandResponse::GroupAckNotInFlight { consumer, offset }
        };
    };
    if delivery.member != member
        || (!delivery_token.is_empty() && delivery.delivery_token != delivery_token)
    {
        return CommandResponse::GroupStaleDelivery { consumer, offset };
    }

    acknowledge_group_offset(consumer_state, offset);
    CommandResponse::GroupAcknowledged
}

fn acknowledge_group_offset(consumer_state: &mut GroupConsumerState, offset: Offset) {
    consumer_state.in_flight.remove(&offset);
    consumer_state.delivery_attempts.remove(&offset);
    if offset == consumer_state.committed_offset {
        consumer_state.committed_offset = consumer_state.committed_offset.saturating_add(1);
        while consumer_state
            .acknowledged_offsets
            .remove(&consumer_state.committed_offset)
        {
            consumer_state.committed_offset = consumer_state.committed_offset.saturating_add(1);
        }
    } else {
        consumer_state.acknowledged_offsets.insert(offset);
    }
}

fn observe_lease_clock(state: &mut SnapshotState, observed_ms: u64) -> u64 {
    state.lease_clock_ms = state.lease_clock_ms.max(observed_ms);
    state.lease_clock_ms
}

pub(super) fn lease_expired(deadline_ms: u64, now_ms: u64) -> bool {
    deadline_ms <= now_ms
}

pub(super) fn dead_letter_stream_name(stream: &str) -> String {
    let name = format!("{stream}{DEAD_LETTER_SUFFIX}");
    if name.len() <= 128 {
        return name;
    }
    let hash = stream.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("{DEAD_LETTER_HASH_PREFIX}{hash:016x}")
}

pub(super) fn is_dead_letter_stream(stream: &str) -> bool {
    stream.ends_with(DEAD_LETTER_SUFFIX) || stream.starts_with(DEAD_LETTER_HASH_PREFIX)
}
