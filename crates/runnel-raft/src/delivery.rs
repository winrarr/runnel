use std::collections::{BTreeMap, BTreeSet};

use runnel_engine::{
    AckBatchItem, AckBatchOutcome, AckBatchRejection, AckBatchResult, ConsumerPolicy,
    DeliveryReceipt, Message, Offset, PollBatchResponseSizer, PollResult, poll_batch_response_len,
    poll_message_response_upper_bound_parts,
};
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
    pub(super) requires_receipt: bool,
    pub(super) deadline_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct GroupRetrySchedule {
    pub(super) retry_not_before_ms: u64,
    pub(super) key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct GroupConsumerState {
    pub(super) committed_offset: Offset,
    pub(super) acknowledged_offsets: BTreeSet<Offset>,
    pub(super) delivery_attempts: BTreeMap<Offset, u32>,
    pub(super) policy: Option<ConsumerPolicy>,
    pub(super) delivery_policies: BTreeMap<Offset, ConsumerPolicy>,
    pub(super) in_flight: BTreeMap<Offset, GroupDelivery>,
    pub(super) retry_not_before: BTreeMap<Offset, GroupRetrySchedule>,
}

impl GroupConsumerState {
    pub(super) fn validate_pinned_policies(&self) -> Result<(), &'static str> {
        if self
            .delivery_attempts
            .keys()
            .ne(self.delivery_policies.keys())
        {
            return Err("delivery attempts and pinned policies must have identical offsets");
        }
        if self.delivery_attempts.values().any(|attempt| *attempt == 0) {
            return Err("delivery attempts must be positive");
        }
        if self.in_flight.iter().any(|(offset, delivery)| {
            self.delivery_attempts.get(offset) != Some(&delivery.delivery_attempt)
        }) {
            return Err("in-flight delivery must match its pinned attempt");
        }
        if self.retry_not_before.keys().any(|offset| {
            !self.delivery_attempts.contains_key(offset) || self.in_flight.contains_key(offset)
        }) {
            return Err("retry schedule must belong to a non-in-flight attempted offset");
        }
        if self
            .in_flight
            .keys()
            .any(|offset| *offset < self.committed_offset)
            || self
                .delivery_attempts
                .keys()
                .any(|offset| *offset < self.committed_offset)
        {
            return Err("uncommitted delivery state must be at or after the committed offset");
        }
        Ok(())
    }
}

pub(super) struct GroupPollRequest {
    pub(super) stream: String,
    pub(super) consumer: String,
    pub(super) member: String,
    pub(super) response_member: Option<String>,
    pub(super) max_response_bytes: usize,
    pub(super) now_ms: u64,
    pub(super) lease_deadline_ms: u64,
    pub(super) max_delivery_attempts: Option<u32>,
    pub(super) legacy_ack_timeout_ms: Option<u64>,
    pub(super) policy_version: Option<u64>,
}

pub(super) struct GroupAckRequest {
    pub(super) stream: String,
    pub(super) consumer: String,
    pub(super) member: String,
    pub(super) offset: Offset,
    pub(super) delivery_token: String,
    pub(super) now_ms: u64,
}

#[derive(Clone)]
pub(super) struct GroupBatchPollRequest {
    pub(super) stream: String,
    pub(super) consumer: String,
    pub(super) member: String,
    pub(super) response_member: Option<String>,
    pub(super) max_records: usize,
    pub(super) max_bytes: usize,
    pub(super) token_seed: String,
    pub(super) now_ms: u64,
    pub(super) lease_deadline_ms: u64,
    pub(super) max_delivery_attempts: Option<u32>,
    pub(super) legacy_ack_timeout_ms: Option<u64>,
    pub(super) policy_version: Option<u64>,
    pub(super) transition_only: bool,
}

pub(super) struct GroupBatchAckRequest {
    pub(super) stream: String,
    pub(super) consumer: String,
    pub(super) member: String,
    pub(super) receipts: Vec<DeliveryReceipt>,
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
    if !state
        .streams
        .get(&request.stream)
        .is_some_and(StreamState::is_active)
    {
        return CommandResponse::StreamNotFound;
    }
    if group_poll_response_upper_bound(state, &request, log_id)
        .is_some_and(|response_bytes| response_bytes > request.max_response_bytes)
    {
        return CommandResponse::GroupPollResponseTooLarge {
            max_bytes: request.max_response_bytes,
        };
    }
    let GroupPollRequest {
        stream,
        consumer,
        member,
        response_member: _,
        max_response_bytes: _,
        now_ms,
        lease_deadline_ms,
        max_delivery_attempts,
        legacy_ack_timeout_ms,
        policy_version,
    } = request;
    let now_ms = observe_lease_clock(state, now_ms);
    let legacy_policy = ConsumerPolicy::legacy(
        legacy_ack_timeout_ms.unwrap_or_default(),
        max_delivery_attempts,
    );

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
    observe_expired_deliveries(state, &stream, &consumer, now_ms, &legacy_policy);

    let existing = state
        .group_consumers
        .get(&consumer_key)
        .expect("group consumer state was initialized above")
        .in_flight
        .iter()
        .find(|(_, delivery)| delivery.member == member)
        .map(|(offset, delivery)| (*offset, delivery.clone()));

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
            let (retry_offsets, retry_keys) = retry_filter(consumer_state, now_ms);
            stream_state
                .messages
                .iter()
                .enumerate()
                .map(|(offset, message)| (offset as Offset, message))
                .filter(|(offset, _)| *offset >= consumer_state.committed_offset)
                .find(|(offset, message)| {
                    if consumer_state.acknowledged_offsets.contains(offset)
                        || consumer_state.in_flight.contains_key(offset)
                        || retry_offsets.contains(offset)
                    {
                        return false;
                    }
                    message.key.as_ref().is_none_or(|key| {
                        !retry_keys.contains(key)
                            && !consumer_state
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
        let policy = group_policy_for_offset(
            state
                .group_consumers
                .get(&consumer_key)
                .expect("group consumer state was initialized above"),
            offset,
            attempts,
            &legacy_policy,
            policy_version,
        );
        let effective_deadline_ms = if policy_version != policy.configured.then_some(policy.version)
        {
            now_ms.saturating_add(policy.ack_timeout_ms)
        } else {
            lease_deadline_ms
        };
        if policy
            .max_delivery_attempts
            .is_some_and(|max| attempts >= max)
            && !is_dead_letter_stream(state, &stream)
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
            consumer_state
                .delivery_policies
                .entry(offset)
                .or_insert_with(|| policy.clone());
            consumer_state.retry_not_before.remove(&offset);
            let delivery = GroupDelivery {
                member: member.clone(),
                key,
                delivery_attempt: *delivery_attempt,
                delivery_token: format!("raft-{log_id}"),
                requires_receipt: false,
                deadline_ms: effective_deadline_ms,
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

pub(super) fn apply_group_batch_poll(
    state: &mut SnapshotState,
    request: GroupBatchPollRequest,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let GroupBatchPollRequest {
        stream,
        consumer,
        member,
        response_member,
        max_records,
        max_bytes,
        token_seed,
        now_ms,
        lease_deadline_ms,
        max_delivery_attempts,
        legacy_ack_timeout_ms,
        policy_version,
        transition_only,
    } = request;
    if max_records == 0 || max_bytes == 0 {
        return CommandResponse::GroupBatchPollActiveSetLimitExceeded;
    }
    if !state
        .streams
        .get(&stream)
        .is_some_and(StreamState::is_active)
    {
        return CommandResponse::StreamNotFound;
    }
    let now_ms = observe_lease_clock(state, now_ms);
    let legacy_policy = ConsumerPolicy::legacy(
        legacy_ack_timeout_ms.unwrap_or_default(),
        max_delivery_attempts,
    );
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

    observe_expired_deliveries(state, &stream, &consumer, now_ms, &legacy_policy);

    let existing = state
        .group_consumers
        .get(&consumer_key)
        .expect("group consumer state was initialized above")
        .in_flight
        .iter()
        .filter(|(_, delivery)| delivery.member == member)
        .map(|(offset, delivery)| (*offset, delivery.clone()))
        .collect::<Vec<_>>();
    if !existing.is_empty() {
        if existing.len() > max_records {
            return CommandResponse::GroupBatchPollActiveSetLimitExceeded;
        }
        let messages = existing
            .iter()
            .filter_map(|(offset, delivery)| {
                let stored = state
                    .streams
                    .get(&stream)
                    .and_then(|stream_state| stream_state.messages.get(*offset as usize))?;
                Some(Message {
                    stream: stream.clone(),
                    offset: *offset,
                    key: stored.key.clone(),
                    payload: stored.payload.clone(),
                    published_at_ms: stored.published_at_ms,
                    delivery_token: Some(delivery.delivery_token.clone()),
                    delivery_attempt: Some(delivery.delivery_attempt),
                })
            })
            .collect::<Vec<_>>();
        if messages.len() != existing.len()
            || poll_batch_response_len(&stream, &consumer, response_member.as_deref(), &messages)
                > max_bytes
        {
            return CommandResponse::GroupBatchPollActiveSetLimitExceeded;
        }
        return CommandResponse::GroupBatchPoll {
            messages,
            collection_complete: true,
            terminal_transitions: 0,
        };
    }

    let mut selected_offsets = BTreeSet::new();
    let mut selected_keys = BTreeSet::new();
    let mut pending = Vec::with_capacity(max_records);
    let mut sizer = PollBatchResponseSizer::new(&stream, &consumer, response_member.as_deref());
    let mut stopped_at_byte_limit = false;
    let mut terminal_transitions = 0usize;

    while pending.len() < max_records && sizer.encoded_len() < max_bytes {
        let candidate = {
            let consumer_state = state
                .group_consumers
                .get(&consumer_key)
                .expect("group consumer state was initialized above");
            let stream_state = state
                .streams
                .get(&stream)
                .expect("stream was checked above");
            let (retry_offsets, retry_keys) = retry_filter(consumer_state, now_ms);
            stream_state
                .messages
                .iter()
                .enumerate()
                .map(|(offset, message)| (offset as Offset, message))
                .filter(|(offset, _)| *offset >= consumer_state.committed_offset)
                .find(|(offset, message)| {
                    if consumer_state.acknowledged_offsets.contains(offset)
                        || consumer_state.in_flight.contains_key(offset)
                        || retry_offsets.contains(offset)
                        || selected_offsets.contains(offset)
                    {
                        return false;
                    }
                    message.key.as_ref().is_none_or(|key| {
                        !selected_keys.contains(key)
                            && !retry_keys.contains(key)
                            && !consumer_state
                                .in_flight
                                .values()
                                .any(|delivery| delivery.key.as_ref() == Some(key))
                    })
                })
                .map(|(offset, message)| (offset, message.key.clone()))
        };
        let Some((offset, key)) = candidate else {
            break;
        };

        let consumer_state = state
            .group_consumers
            .get(&consumer_key)
            .expect("group consumer state was initialized above");
        let attempts = consumer_state
            .delivery_attempts
            .get(&offset)
            .copied()
            .unwrap_or_default();
        let policy = group_policy_for_offset(
            consumer_state,
            offset,
            attempts,
            &legacy_policy,
            policy_version,
        );
        if policy
            .max_delivery_attempts
            .is_some_and(|maximum| attempts >= maximum)
            && !is_dead_letter_stream(state, &stream)
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
            terminal_transitions = terminal_transitions.saturating_add(1);
            continue;
        }

        if transition_only {
            break;
        }

        let delivery_attempt = attempts.saturating_add(1);
        let message = state
            .streams
            .get(&stream)
            .and_then(|stream_state| stream_state.messages.get(offset as usize))
            .map(|stored| Message {
                stream: stream.clone(),
                offset,
                key: stored.key.clone(),
                payload: stored.payload.clone(),
                published_at_ms: stored.published_at_ms,
                delivery_token: Some(format!("{token_seed}-{offset:x}")),
                delivery_attempt: Some(delivery_attempt),
            })
            .expect("candidate offset must refer to a stored message");
        if sizer.projected_len(&message) > max_bytes {
            if pending.is_empty() {
                return CommandResponse::GroupBatchPollRecordTooLarge { max_bytes };
            }
            stopped_at_byte_limit = true;
            break;
        }
        sizer.push(&message);
        selected_offsets.insert(offset);
        if let Some(key) = key.as_ref() {
            selected_keys.insert(key.clone());
        }
        let effective_deadline_ms = if policy_version != policy.configured.then_some(policy.version)
        {
            now_ms.saturating_add(policy.ack_timeout_ms)
        } else {
            lease_deadline_ms
        };
        pending.push((
            message,
            GroupDelivery {
                member: member.clone(),
                key,
                delivery_attempt,
                delivery_token: format!("{token_seed}-{offset:x}"),
                requires_receipt: true,
                deadline_ms: effective_deadline_ms,
            },
            policy,
        ));
    }

    if pending.is_empty() {
        return CommandResponse::GroupBatchPoll {
            messages: Vec::new(),
            collection_complete: false,
            terminal_transitions,
        };
    }

    let record_limit_reached = pending.len() == max_records;
    let consumer_state = state
        .group_consumers
        .get_mut(&consumer_key)
        .expect("group consumer state was initialized above");
    let mut messages = Vec::with_capacity(pending.len());
    for (message, delivery, policy) in pending {
        let offset = message.offset;
        let attempt = consumer_state
            .delivery_attempts
            .entry(offset)
            .and_modify(|attempt| *attempt = (*attempt).max(delivery.delivery_attempt))
            .or_insert(delivery.delivery_attempt);
        consumer_state
            .delivery_policies
            .entry(offset)
            .or_insert(policy);
        consumer_state.retry_not_before.remove(&offset);
        consumer_state.in_flight.insert(offset, delivery);
        if *attempt > 1 {
            state.redeliveries = state.redeliveries.saturating_add(1);
        }
        messages.push(message);
    }
    CommandResponse::GroupBatchPoll {
        messages,
        collection_complete: record_limit_reached || stopped_at_byte_limit,
        terminal_transitions,
    }
}

pub(super) fn preview_group_batch(
    state: &SnapshotState,
    request: GroupBatchPollRequest,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    if request.max_records == 0 || request.max_bytes == 0 {
        return CommandResponse::GroupBatchPollActiveSetLimitExceeded;
    }
    let Some(stream_state) = state
        .streams
        .get(&request.stream)
        .filter(|stream| stream.is_active())
    else {
        return CommandResponse::StreamNotFound;
    };

    let now_ms = state.lease_clock_ms.max(request.now_ms);
    let consumer_key = (request.stream.clone(), request.consumer.clone());
    let consumer_state = state.group_consumers.get(&consumer_key);
    let committed_offset = consumer_state.map_or_else(
        || {
            state
                .consumers
                .get(&consumer_key)
                .copied()
                .unwrap_or_default()
        },
        |consumer| consumer.committed_offset,
    );
    let live_deliveries = consumer_state.into_iter().flat_map(|consumer| {
        consumer
            .in_flight
            .iter()
            .filter(move |(_, delivery)| !lease_expired(delivery.deadline_ms, now_ms))
    });
    let existing = live_deliveries
        .filter(|(_, delivery)| delivery.member == request.member)
        .collect::<Vec<_>>();
    if !existing.is_empty() {
        if existing.len() > request.max_records {
            return CommandResponse::GroupBatchPollActiveSetLimitExceeded;
        }
        let messages = existing
            .iter()
            .filter_map(|(offset, delivery)| {
                let stored = stream_state.messages.get(**offset as usize)?;
                Some(Message {
                    stream: request.stream.clone(),
                    offset: **offset,
                    key: stored.key.clone(),
                    payload: stored.payload.clone(),
                    published_at_ms: stored.published_at_ms,
                    delivery_token: Some(delivery.delivery_token.clone()),
                    delivery_attempt: Some(delivery.delivery_attempt),
                })
            })
            .collect::<Vec<_>>();
        if messages.len() != existing.len()
            || poll_batch_response_len(
                &request.stream,
                &request.consumer,
                request.response_member.as_deref(),
                &messages,
            ) > request.max_bytes
        {
            return CommandResponse::GroupBatchPollActiveSetLimitExceeded;
        }
        return CommandResponse::GroupBatchPoll {
            messages,
            collection_complete: true,
            terminal_transitions: 0,
        };
    }

    let legacy_policy = ConsumerPolicy::legacy(
        request.legacy_ack_timeout_ms.unwrap_or_default(),
        request.max_delivery_attempts,
    );
    let (mut retry_offsets, mut retry_keys) = consumer_state
        .map(|consumer| retry_filter(consumer, now_ms))
        .unwrap_or_default();
    if let Some(consumer) = consumer_state {
        for (&offset, delivery) in &consumer.in_flight {
            if !lease_expired(delivery.deadline_ms, now_ms)
                || consumer.retry_not_before.contains_key(&offset)
            {
                continue;
            }
            let attempts = consumer
                .delivery_attempts
                .get(&offset)
                .copied()
                .unwrap_or_default();
            let policy = group_policy_for_offset(
                consumer,
                offset,
                attempts,
                &legacy_policy,
                request.policy_version,
            );
            if policy.retry_delay_ms == 0
                || policy
                    .max_delivery_attempts
                    .is_some_and(|maximum| attempts >= maximum)
            {
                continue;
            }
            retry_offsets.insert(offset);
            if let Some(key) = delivery.key.as_ref() {
                retry_keys.insert(key.clone());
            }
        }
    }
    let mut selected_offsets = BTreeSet::new();
    let mut selected_keys = BTreeSet::new();
    let mut terminal_offsets = BTreeSet::new();
    let mut messages = Vec::with_capacity(request.max_records);
    let mut sizer = PollBatchResponseSizer::new(
        &request.stream,
        &request.consumer,
        request.response_member.as_deref(),
    );
    let mut stopped_at_byte_limit = false;
    let mut terminal_transitions = 0usize;
    let is_dead_letter = is_dead_letter_stream(state, &request.stream);

    while messages.len() < request.max_records && sizer.encoded_len() < request.max_bytes {
        let effective_committed_offset =
            advance_preview_committed_offset(committed_offset, consumer_state, &terminal_offsets);
        let candidate = stream_state
            .messages
            .iter()
            .enumerate()
            .map(|(offset, message)| (offset as Offset, message))
            .filter(|(offset, _)| *offset >= effective_committed_offset)
            .find(|(offset, message)| {
                if consumer_state.is_some_and(|consumer| {
                    consumer.acknowledged_offsets.contains(offset)
                        || retry_offsets.contains(offset)
                        || consumer
                            .in_flight
                            .get(offset)
                            .is_some_and(|delivery| !lease_expired(delivery.deadline_ms, now_ms))
                }) || terminal_offsets.contains(offset)
                    || selected_offsets.contains(offset)
                {
                    return false;
                }
                message.key.as_ref().is_none_or(|key| {
                    !selected_keys.contains(key)
                        && !retry_keys.contains(key)
                        && !consumer_state.is_some_and(|consumer| {
                            consumer.in_flight.values().any(|delivery| {
                                !lease_expired(delivery.deadline_ms, now_ms)
                                    && delivery.key.as_ref() == Some(key)
                            })
                        })
                })
            });
        let Some((offset, stored)) = candidate else {
            break;
        };

        let attempts = consumer_state
            .and_then(|consumer| consumer.delivery_attempts.get(&offset).copied())
            .unwrap_or_default();
        let policy = consumer_state.map_or_else(
            || legacy_policy.clone(),
            |consumer| {
                group_policy_for_offset(
                    consumer,
                    offset,
                    attempts,
                    &legacy_policy,
                    request.policy_version,
                )
            },
        );
        if policy
            .max_delivery_attempts
            .is_some_and(|maximum| attempts >= maximum)
            && !is_dead_letter
        {
            terminal_offsets.insert(offset);
            terminal_transitions = terminal_transitions.saturating_add(1);
            continue;
        }
        if request.transition_only {
            break;
        }

        let message = Message {
            stream: request.stream.clone(),
            offset,
            key: stored.key.clone(),
            payload: stored.payload.clone(),
            published_at_ms: stored.published_at_ms,
            delivery_token: Some(format!("{}-{offset:x}", request.token_seed)),
            delivery_attempt: Some(attempts.saturating_add(1)),
        };
        if sizer.projected_len(&message) > request.max_bytes {
            if messages.is_empty() {
                return CommandResponse::GroupBatchPollRecordTooLarge {
                    max_bytes: request.max_bytes,
                };
            }
            stopped_at_byte_limit = true;
            break;
        }
        sizer.push(&message);
        selected_offsets.insert(offset);
        if let Some(key) = stored.key.as_ref() {
            selected_keys.insert(key.clone());
        }
        messages.push(message);
    }

    CommandResponse::GroupBatchPoll {
        collection_complete: !messages.is_empty()
            && (messages.len() == request.max_records || stopped_at_byte_limit),
        messages,
        terminal_transitions,
    }
}

fn advance_preview_committed_offset(
    mut committed_offset: Offset,
    consumer: Option<&GroupConsumerState>,
    terminal_offsets: &BTreeSet<Offset>,
) -> Offset {
    loop {
        let acknowledged = consumer
            .is_some_and(|consumer| consumer.acknowledged_offsets.contains(&committed_offset));
        if !acknowledged && !terminal_offsets.contains(&committed_offset) {
            return committed_offset;
        }
        let Some(next_offset) = committed_offset.checked_add(1) else {
            return committed_offset;
        };
        committed_offset = next_offset;
    }
}

pub(super) fn group_policy_for_offset(
    consumer_state: &GroupConsumerState,
    offset: Offset,
    attempts: u32,
    legacy_policy: &ConsumerPolicy,
    policy_version: Option<u64>,
) -> ConsumerPolicy {
    let policy = if attempts > 0 {
        consumer_state
            .delivery_policies
            .get(&offset)
            .expect("every attempted offset has a pinned policy")
            .clone()
    } else {
        consumer_state
            .delivery_policies
            .get(&offset)
            .or(consumer_state.policy.as_ref())
            .cloned()
            .unwrap_or_else(|| legacy_policy.clone())
    };
    if policy_version.is_some_and(|version| {
        consumer_state
            .policy
            .as_ref()
            .is_some_and(|current| current.version == version)
    }) {
        return policy;
    }
    if attempts == 0 {
        return consumer_state.policy.clone().unwrap_or(policy);
    }
    policy
}

pub(super) fn apply_group_batch_ack(
    state: &mut SnapshotState,
    request: GroupBatchAckRequest,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let GroupBatchAckRequest {
        stream,
        consumer,
        member,
        receipts,
        now_ms,
    } = request;
    if !state
        .streams
        .get(&stream)
        .is_some_and(StreamState::is_active)
    {
        return CommandResponse::StreamNotFound;
    }
    let now_ms = observe_lease_clock(state, now_ms);
    let legacy_policy = ConsumerPolicy::legacy(0, None);
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
    observe_expired_deliveries(state, &stream, &consumer, now_ms, &legacy_policy);
    let consumer_state = state
        .group_consumers
        .get_mut(&consumer_key)
        .expect("group consumer state was initialized above");

    let mut valid_offsets = Vec::new();
    let mut outcomes = Vec::with_capacity(receipts.len());
    for receipt in receipts {
        let outcome = if receipt.offset < consumer_state.committed_offset
            || consumer_state
                .acknowledged_offsets
                .contains(&receipt.offset)
        {
            AckBatchOutcome::AlreadyConfirmed
        } else if receipt.delivery_token.is_empty() {
            AckBatchOutcome::Rejected {
                reason: AckBatchRejection::StaleDelivery,
            }
        } else if let Some(delivery) = consumer_state.in_flight.get(&receipt.offset) {
            if delivery.member != member || delivery.delivery_token != receipt.delivery_token {
                AckBatchOutcome::Rejected {
                    reason: AckBatchRejection::StaleDelivery,
                }
            } else {
                valid_offsets.push(receipt.offset);
                AckBatchOutcome::Confirmed
            }
        } else if consumer_state
            .delivery_attempts
            .contains_key(&receipt.offset)
        {
            AckBatchOutcome::Rejected {
                reason: AckBatchRejection::StaleDelivery,
            }
        } else {
            AckBatchOutcome::Rejected {
                reason: AckBatchRejection::NotInFlight,
            }
        };
        outcomes.push(AckBatchItem {
            offset: receipt.offset,
            outcome,
        });
    }
    for offset in valid_offsets {
        acknowledge_group_offset(consumer_state, offset);
    }
    CommandResponse::GroupBatchAcknowledged {
        result: AckBatchResult { outcomes },
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

fn group_poll_response_upper_bound(
    state: &SnapshotState,
    request: &GroupPollRequest,
    log_id: openraft::LogId<super::NodeId>,
) -> Option<usize> {
    let stream_state = state.streams.get(&request.stream)?;
    let consumer_key = (request.stream.clone(), request.consumer.clone());
    let now_ms = state.lease_clock_ms.max(request.now_ms);
    let mut consumer_state = state
        .group_consumers
        .get(&consumer_key)
        .cloned()
        .unwrap_or_else(|| GroupConsumerState {
            committed_offset: state
                .consumers
                .get(&consumer_key)
                .copied()
                .unwrap_or_default(),
            ..GroupConsumerState::default()
        });
    consumer_state
        .in_flight
        .retain(|_, delivery| !lease_expired(delivery.deadline_ms, now_ms));

    if let Some((offset, delivery)) = consumer_state
        .in_flight
        .iter()
        .find(|(_, delivery)| delivery.member == request.member)
    {
        let stored = stream_state.messages.get(*offset as usize)?;
        return Some(poll_message_response_upper_bound_parts(
            &request.stream,
            &request.consumer,
            request.response_member.as_deref(),
            *offset,
            stored.key.as_deref(),
            stored.payload.len(),
            stored.published_at_ms,
            Some(&delivery.delivery_token),
            Some(delivery.delivery_attempt),
            request.response_member.is_some(),
        ));
    }

    let legacy_policy = ConsumerPolicy::legacy(
        request.legacy_ack_timeout_ms.unwrap_or_default(),
        request.max_delivery_attempts,
    );
    let token = format!("raft-{log_id}");
    loop {
        let candidate = stream_state
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
            });
        let (offset, stored) = candidate?;
        let attempts = consumer_state
            .delivery_attempts
            .get(&offset)
            .copied()
            .unwrap_or_default();
        let policy = group_policy_for_offset(
            &consumer_state,
            offset,
            attempts,
            &legacy_policy,
            request.policy_version,
        );
        if policy
            .max_delivery_attempts
            .is_some_and(|maximum| attempts >= maximum)
            && !is_dead_letter_stream(state, &request.stream)
        {
            acknowledge_group_offset(&mut consumer_state, offset);
            continue;
        }
        return Some(poll_message_response_upper_bound_parts(
            &request.stream,
            &request.consumer,
            request.response_member.as_deref(),
            offset,
            stored.key.as_deref(),
            stored.payload.len(),
            stored.published_at_ms,
            Some(&token),
            Some(attempts.saturating_add(1)),
            request.response_member.is_some(),
        ));
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
    let legacy_policy = ConsumerPolicy::legacy(0, None);
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
    observe_expired_deliveries(state, &stream, &consumer, now_ms, &legacy_policy);
    let consumer_state = state.group_consumers.entry(consumer_key).or_default();

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
        || ((delivery.requires_receipt || !delivery_token.is_empty())
            && delivery.delivery_token != delivery_token)
    {
        return CommandResponse::GroupStaleDelivery { consumer, offset };
    }

    acknowledge_group_offset(consumer_state, offset);
    CommandResponse::GroupAcknowledged
}

fn observe_expired_deliveries(
    state: &mut SnapshotState,
    stream: &str,
    consumer: &str,
    now_ms: u64,
    legacy_policy: &ConsumerPolicy,
) {
    let consumer_state = state
        .group_consumers
        .get_mut(&(stream.to_owned(), consumer.to_owned()))
        .expect("group consumer state was initialized above");
    let expired = consumer_state
        .in_flight
        .iter()
        .filter_map(|(&offset, delivery)| {
            lease_expired(delivery.deadline_ms, now_ms).then_some((offset, delivery.clone()))
        })
        .collect::<Vec<_>>();
    for (offset, delivery) in expired {
        consumer_state.in_flight.remove(&offset);
        if consumer_state.retry_not_before.contains_key(&offset) {
            continue;
        }
        let attempts = consumer_state
            .delivery_attempts
            .get(&offset)
            .copied()
            .unwrap_or_default();
        let policy = group_policy_for_offset(consumer_state, offset, attempts, legacy_policy, None);
        if policy
            .max_delivery_attempts
            .is_some_and(|maximum| attempts >= maximum)
            || policy.retry_delay_ms == 0
        {
            continue;
        }
        consumer_state.retry_not_before.insert(
            offset,
            GroupRetrySchedule {
                retry_not_before_ms: now_ms.saturating_add(policy.retry_delay_ms),
                key: delivery.key,
            },
        );
    }
}

fn retry_filter(
    consumer_state: &GroupConsumerState,
    now_ms: u64,
) -> (BTreeSet<Offset>, BTreeSet<String>) {
    let mut offsets = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for (&offset, schedule) in &consumer_state.retry_not_before {
        if schedule.retry_not_before_ms <= now_ms {
            continue;
        }
        offsets.insert(offset);
        if let Some(key) = schedule.key.as_ref() {
            keys.insert(key.clone());
        }
    }
    (offsets, keys)
}

fn acknowledge_group_offset(consumer_state: &mut GroupConsumerState, offset: Offset) {
    consumer_state.in_flight.remove(&offset);
    consumer_state.delivery_attempts.remove(&offset);
    consumer_state.delivery_policies.remove(&offset);
    consumer_state.retry_not_before.remove(&offset);
    if offset == consumer_state.committed_offset {
        consumer_state.committed_offset = consumer_state.committed_offset.saturating_add(1);
        while consumer_state
            .acknowledged_offsets
            .remove(&consumer_state.committed_offset)
        {
            consumer_state
                .delivery_policies
                .remove(&consumer_state.committed_offset);
            consumer_state
                .retry_not_before
                .remove(&consumer_state.committed_offset);
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

fn is_dead_letter_stream(state: &SnapshotState, stream: &str) -> bool {
    if stream.ends_with(DEAD_LETTER_SUFFIX) {
        return true;
    }
    if !stream.starts_with(DEAD_LETTER_HASH_PREFIX) {
        return false;
    }

    state
        .streams
        .keys()
        .any(|source| dead_letter_stream_name(source) == stream)
}

#[cfg(test)]
mod retry_delay_tests {
    use super::*;
    use crate::state_machine::{SnapshotState, StoredMessage, StreamState};

    fn state(max_attempts: Option<u32>, retry_delay_ms: u64) -> SnapshotState {
        let mut state = SnapshotState::default();
        let mut stream = StreamState::active("events-id".to_owned(), "group-id".to_owned());
        stream.messages.push(StoredMessage {
            key: Some("same".to_owned()),
            payload: b"first".to_vec(),
            published_at_ms: 1,
        });
        stream.messages.push(StoredMessage {
            key: Some("same".to_owned()),
            payload: b"successor".to_vec(),
            published_at_ms: 2,
        });
        stream.messages.push(StoredMessage {
            key: Some("other".to_owned()),
            payload: b"unrelated".to_vec(),
            published_at_ms: 3,
        });
        state.streams.insert("events".to_owned(), stream);
        state.group_consumers.insert(
            ("events".to_owned(), "workers".to_owned()),
            GroupConsumerState {
                policy: Some(ConsumerPolicy::configured(
                    1,
                    10,
                    max_attempts,
                    retry_delay_ms,
                )),
                ..GroupConsumerState::default()
            },
        );
        state
    }

    fn log_id(index: u64) -> openraft::LogId<crate::NodeId> {
        openraft::LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1),
            index,
        }
    }

    fn poll_request(now_ms: u64, lease_deadline_ms: u64, member: &str) -> GroupPollRequest {
        GroupPollRequest {
            stream: "events".to_owned(),
            consumer: "workers".to_owned(),
            member: member.to_owned(),
            response_member: None,
            max_response_bytes: runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
            now_ms,
            lease_deadline_ms,
            max_delivery_attempts: None,
            legacy_ack_timeout_ms: Some(10),
            policy_version: Some(1),
        }
    }

    fn message(response: CommandResponse) -> Message {
        let CommandResponse::GroupPoll {
            result: PollResult::Message(message),
        } = response
        else {
            panic!("expected grouped message")
        };
        message
    }

    #[test]
    fn retry_deadline_is_pinned_persisted_and_keeps_same_key_reserved() {
        let mut state = state(None, 100);
        let first = message(apply_group_poll(
            &mut state,
            poll_request(100, 110, "member-a"),
            log_id(1),
            &GroupKind::Combined,
        ));
        assert_eq!(first.offset, 0);

        let response = apply_group_ack(
            &mut state,
            GroupAckRequest {
                stream: "events".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first.delivery_token.unwrap(),
                now_ms: 110,
            },
            &GroupKind::Combined,
        );
        assert_eq!(
            response,
            CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }
        );

        // A later configuration applies only to records without a pinned attempt policy.
        state
            .group_consumers
            .get_mut(&("events".to_owned(), "workers".to_owned()))
            .unwrap()
            .policy = Some(ConsumerPolicy::configured(2, 10, None, 0));
        let consumer_key = ("events".to_owned(), "workers".to_owned());
        let persisted_consumer: GroupConsumerState = serde_json::from_slice(
            &serde_json::to_vec(state.group_consumers.get(&consumer_key).unwrap()).unwrap(),
        )
        .unwrap();
        state
            .group_consumers
            .insert(consumer_key.clone(), persisted_consumer);
        assert_eq!(
            state.group_consumers[&consumer_key].retry_not_before[&0].retry_not_before_ms,
            210
        );

        let unrelated = message(apply_group_poll(
            &mut state,
            poll_request(209, 219, "member-b"),
            log_id(3),
            &GroupKind::Combined,
        ));
        assert_eq!(unrelated.offset, 2);
        assert_eq!(
            apply_group_poll(
                &mut state,
                poll_request(209, 219, "member-c"),
                log_id(4),
                &GroupKind::Combined,
            ),
            CommandResponse::GroupPoll {
                result: PollResult::Empty,
            }
        );
        let retry = message(apply_group_poll(
            &mut state,
            poll_request(210, 220, "member-d"),
            log_id(5),
            &GroupKind::Combined,
        ));
        assert_eq!(retry.offset, 0);
        assert_eq!(retry.delivery_attempt, Some(2));
    }

    #[test]
    fn batch_retry_deadline_reserves_its_key_but_allows_unrelated_work() {
        let mut state = state(None, 100);
        let first = apply_group_batch_poll(
            &mut state,
            GroupBatchPollRequest {
                stream: "events".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                response_member: None,
                max_records: 1,
                max_bytes: 65_536,
                token_seed: "first".to_owned(),
                now_ms: 100,
                lease_deadline_ms: 110,
                max_delivery_attempts: None,
                legacy_ack_timeout_ms: Some(10),
                policy_version: Some(1),
                transition_only: false,
            },
            &GroupKind::Combined,
        );
        let first_token = match first {
            CommandResponse::GroupBatchPoll { messages, .. } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].offset, 0);
                messages[0].delivery_token.clone().unwrap()
            }
            response => panic!("expected first batch delivery, got {response:?}"),
        };

        let acknowledgement = apply_group_batch_ack(
            &mut state,
            GroupBatchAckRequest {
                stream: "events".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                receipts: vec![DeliveryReceipt {
                    offset: 0,
                    delivery_token: first_token,
                }],
                now_ms: 110,
            },
            &GroupKind::Combined,
        );
        assert!(matches!(
            acknowledgement,
            CommandResponse::GroupBatchAcknowledged { result }
                if matches!(result.outcomes[0].outcome, AckBatchOutcome::Rejected {
                    reason: AckBatchRejection::StaleDelivery
                })
        ));

        let consumer_key = ("events".to_owned(), "workers".to_owned());
        let persisted_consumer: GroupConsumerState = serde_json::from_slice(
            &serde_json::to_vec(state.group_consumers.get(&consumer_key).unwrap()).unwrap(),
        )
        .unwrap();
        state
            .group_consumers
            .insert(consumer_key.clone(), persisted_consumer);
        assert_eq!(
            state.group_consumers[&consumer_key].retry_not_before[&0].retry_not_before_ms,
            210
        );

        let poll_batch = |state: &mut SnapshotState, now_ms, member: &str, index| {
            apply_group_batch_poll(
                state,
                GroupBatchPollRequest {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: member.to_owned(),
                    response_member: None,
                    max_records: 1,
                    max_bytes: 65_536,
                    token_seed: format!("batch-{index}"),
                    now_ms,
                    lease_deadline_ms: now_ms + 10,
                    max_delivery_attempts: None,
                    legacy_ack_timeout_ms: Some(10),
                    policy_version: Some(1),
                    transition_only: false,
                },
                &GroupKind::Combined,
            )
        };

        let unrelated = poll_batch(&mut state, 209, "member-b", 2);
        assert!(matches!(
            unrelated,
            CommandResponse::GroupBatchPoll { messages, .. }
                if messages.len() == 1 && messages[0].offset == 2
        ));
        assert!(matches!(
            poll_batch(&mut state, 209, "member-c", 3),
            CommandResponse::GroupBatchPoll { ref messages, .. } if messages.is_empty()
        ));
        assert!(matches!(
            poll_batch(&mut state, 210, "member-d", 4),
            CommandResponse::GroupBatchPoll { ref messages, .. }
                if messages.len() == 1
                    && messages[0].offset == 0
                    && messages[0].delivery_attempt == Some(2)
        ));
    }

    #[test]
    fn zero_retry_delay_keeps_immediate_redelivery_without_a_schedule_entry() {
        let mut state = state(None, 0);
        let first = message(apply_group_poll(
            &mut state,
            poll_request(100, 110, "member-a"),
            log_id(1),
            &GroupKind::Combined,
        ));
        assert_eq!(first.delivery_attempt, Some(1));

        assert_eq!(
            apply_group_ack(
                &mut state,
                GroupAckRequest {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-a".to_owned(),
                    offset: 0,
                    delivery_token: first.delivery_token.unwrap(),
                    now_ms: 110,
                },
                &GroupKind::Combined,
            ),
            CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }
        );

        let consumer = &state.group_consumers[&("events".to_owned(), "workers".to_owned())];
        assert!(!consumer.retry_not_before.contains_key(&0));
        let retry = message(apply_group_poll(
            &mut state,
            poll_request(110, 120, "member-b"),
            log_id(3),
            &GroupKind::Combined,
        ));
        assert_eq!(retry.offset, 0);
        assert_eq!(retry.delivery_attempt, Some(2));
    }

    #[test]
    fn terminal_attempt_is_dead_lettered_at_expiry_without_retry_delay() {
        let mut state = state(Some(1), 100);
        let first = message(apply_group_poll(
            &mut state,
            poll_request(100, 110, "member-a"),
            log_id(1),
            &GroupKind::Combined,
        ));
        assert_eq!(first.delivery_attempt, Some(1));
        let terminal = apply_group_poll(
            &mut state,
            poll_request(110, 120, "member-b"),
            log_id(2),
            &GroupKind::Combined,
        );
        assert_eq!(
            terminal,
            CommandResponse::GroupPoll {
                result: PollResult::Message(Message {
                    stream: "events".to_owned(),
                    offset: 1,
                    key: Some("same".to_owned()),
                    payload: b"successor".to_vec(),
                    published_at_ms: 2,
                    delivery_token: Some(format!("raft-{}", log_id(2))),
                    delivery_attempt: Some(1),
                }),
            }
        );
        let consumer = &state.group_consumers[&("events".to_owned(), "workers".to_owned())];
        assert!(!consumer.retry_not_before.contains_key(&0));
        assert_eq!(state.dead_letters, 1);
        assert_eq!(state.streams["events.dead-letter"].messages.len(), 1);
    }
}
