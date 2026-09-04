use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use runnel_engine::{BrokerError, Offset};

use super::consumer_state::{ConsumerState, load_consumer_state};

pub(super) const MAX_CACHED_CONSUMER_STATES: usize = 1024;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DeliveryKey {
    consumer: String,
    offset: Offset,
}

#[derive(Debug, Clone)]
pub(super) struct InFlight {
    member: String,
    offset: Offset,
    key: Option<String>,
    delivery_attempt: u32,
    delivery_token: String,
    deadline: Instant,
}

impl InFlight {
    pub(super) fn new(
        member: &str,
        offset: Offset,
        key: Option<String>,
        delivery_attempt: u32,
        delivery_token: String,
        deadline: Instant,
    ) -> Self {
        Self {
            member: member.to_owned(),
            offset,
            key,
            delivery_attempt,
            delivery_token,
            deadline,
        }
    }

    pub(super) fn member(&self) -> &str {
        &self.member
    }

    pub(super) fn offset(&self) -> Offset {
        self.offset
    }

    pub(super) fn delivery_attempt(&self) -> u32 {
        self.delivery_attempt
    }

    pub(super) fn delivery_token(&self) -> &str {
        &self.delivery_token
    }
}

#[derive(Default)]
struct ConsumerInFlightIndex {
    offsets: HashSet<Offset>,
    keys: HashSet<String>,
    members: HashMap<String, Offset>,
}

#[derive(Default)]
pub(super) struct DeliveryState {
    // The bounded cache is only a best-effort fast path; the durable checkpoint remains the
    // source of truth across restart and eviction.
    consumer_states: HashMap<String, ConsumerState>,
    in_flight: HashMap<DeliveryKey, InFlight>,
    // This index keeps expiry checks proportional to deliveries that are due instead of scanning
    // every outstanding delivery on each poll. Entries are removed when a delivery is acked.
    in_flight_deadlines: BTreeMap<Instant, Vec<DeliveryKey>>,
    // This index mirrors only active deliveries and is removed when a consumer has no deliveries.
    // It avoids rebuilding per-consumer offset/key sets and scanning members on every poll.
    in_flight_by_consumer: HashMap<String, ConsumerInFlightIndex>,
}

impl DeliveryState {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn expire(&mut self, now: Instant) {
        while let Some((&deadline, _)) = self.in_flight_deadlines.first_key_value() {
            if deadline > now {
                break;
            }

            let Some((_, delivery_keys)) = self.in_flight_deadlines.pop_first() else {
                break;
            };
            for delivery_key in delivery_keys {
                if self
                    .in_flight
                    .get(&delivery_key)
                    .is_some_and(|in_flight| in_flight.deadline <= now)
                {
                    self.remove_in_flight(&delivery_key);
                }
            }
        }
    }

    pub(super) fn member_delivery(
        &self,
        consumer: &str,
        member: &str,
    ) -> Result<Option<InFlight>, BrokerError> {
        let Some(offset) = self
            .in_flight_by_consumer
            .get(consumer)
            .and_then(|index| index.members.get(member))
            .copied()
        else {
            return Ok(None);
        };
        let delivery_key = DeliveryKey {
            consumer: consumer.to_owned(),
            offset,
        };
        self.in_flight
            .get(&delivery_key)
            .cloned()
            .map(Some)
            .ok_or(BrokerError::CorruptRecord(offset))
    }

    pub(super) fn in_flight_filter(
        &self,
        consumer: &str,
    ) -> Option<(&HashSet<Offset>, &HashSet<String>)> {
        self.in_flight_by_consumer
            .get(consumer)
            .map(|index| (&index.offsets, &index.keys))
    }

    pub(super) fn get_in_flight(&self, consumer: &str, offset: Offset) -> Option<&InFlight> {
        self.in_flight.get(&DeliveryKey {
            consumer: consumer.to_owned(),
            offset,
        })
    }

    pub(super) fn insert(&mut self, consumer: &str, in_flight: InFlight) {
        let delivery_key = DeliveryKey {
            consumer: consumer.to_owned(),
            offset: in_flight.offset,
        };
        let offset = in_flight.offset;
        let member = in_flight.member.clone();
        let key = in_flight.key.clone();
        debug_assert!(!self.in_flight.contains_key(&delivery_key));
        let deadline = in_flight.deadline;
        self.in_flight.insert(delivery_key.clone(), in_flight);
        self.in_flight_deadlines
            .entry(deadline)
            .or_default()
            .push(delivery_key);

        let index = self
            .in_flight_by_consumer
            .entry(consumer.to_owned())
            .or_default();
        index.offsets.insert(offset);
        if let Some(key) = key {
            index.keys.insert(key);
        }
        index.members.insert(member, offset);
    }

    pub(super) fn remove(&mut self, consumer: &str, offset: Offset) -> Option<InFlight> {
        self.remove_in_flight(&DeliveryKey {
            consumer: consumer.to_owned(),
            offset,
        })
    }

    pub(super) fn load_consumer_state_for_request(
        &mut self,
        root: &Path,
        stream: &str,
        consumer: &str,
    ) -> Result<ConsumerState, BrokerError> {
        if let Some(cached) = self.consumer_states.get(consumer) {
            return Ok(cached.clone());
        }

        load_consumer_state(root, stream, consumer)
    }

    pub(super) fn cache_consumer_state(&mut self, consumer: String, state: ConsumerState) {
        self.consumer_states.insert(consumer, state);
        while self.consumer_states.len() > MAX_CACHED_CONSUMER_STATES {
            let evicted = {
                let active_consumers: HashSet<&str> = self
                    .in_flight
                    .keys()
                    .map(|delivery_key| delivery_key.consumer.as_str())
                    .collect();
                self.consumer_states
                    .keys()
                    .find(|consumer| !active_consumers.contains((*consumer).as_str()))
                    .cloned()
                    .or_else(|| self.consumer_states.keys().next().cloned())
            };
            let Some(evicted) = evicted else {
                break;
            };
            self.consumer_states.remove(&evicted);
        }
    }

    pub(super) fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    fn remove_in_flight(&mut self, delivery_key: &DeliveryKey) -> Option<InFlight> {
        let in_flight = self.in_flight.remove(delivery_key)?;
        let mut remove_deadline_index = false;
        if let Some(deliveries) = self.in_flight_deadlines.get_mut(&in_flight.deadline) {
            if let Some(index) = deliveries.iter().position(|key| key == delivery_key) {
                deliveries.swap_remove(index);
            }
            remove_deadline_index = deliveries.is_empty();
        }
        if remove_deadline_index {
            self.in_flight_deadlines.remove(&in_flight.deadline);
        }
        let mut remove_consumer_index = false;
        if let Some(index) = self.in_flight_by_consumer.get_mut(&delivery_key.consumer) {
            index.offsets.remove(&delivery_key.offset);
            if let Some(key) = in_flight.key.as_ref() {
                index.keys.remove(key);
            }
            if index
                .members
                .get(&in_flight.member)
                .is_some_and(|offset| *offset == delivery_key.offset)
            {
                index.members.remove(&in_flight.member);
            }
            remove_consumer_index = index.offsets.is_empty();
        }
        if remove_consumer_index {
            self.in_flight_by_consumer.remove(&delivery_key.consumer);
        }
        Some(in_flight)
    }

    #[cfg(test)]
    pub(super) fn consumer_state_cache_len(&self) -> usize {
        self.consumer_states.len()
    }

    #[cfg(test)]
    pub(super) fn has_cached_consumer(&self, consumer: &str) -> bool {
        self.consumer_states.contains_key(consumer)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
            && self.in_flight_deadlines.is_empty()
            && self.in_flight_by_consumer.is_empty()
    }
}

pub(super) struct DeliveryTokenGenerator {
    epoch: u128,
    next_id: AtomicU64,
}

impl DeliveryTokenGenerator {
    pub(super) fn new() -> Self {
        Self {
            epoch: delivery_epoch(),
            next_id: AtomicU64::new(0),
        }
    }

    pub(super) fn next(&self) -> String {
        let next_id = self.next_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        format!("{:x}-{:x}", self.epoch, next_id)
    }
}

fn delivery_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiring_delivery_removes_all_indexes() {
        let deadline = Instant::now();
        let mut state = DeliveryState::new();
        state.insert(
            "workers",
            InFlight::new(
                "member-a",
                7,
                Some("customer-a".to_owned()),
                2,
                "token".to_owned(),
                deadline,
            ),
        );

        assert_eq!(state.in_flight_count(), 1);
        assert!(
            state
                .member_delivery("workers", "member-a")
                .unwrap()
                .is_some()
        );

        state.expire(deadline);

        assert_eq!(state.in_flight_count(), 0);
        assert!(state.in_flight_filter("workers").is_none());
        assert!(
            state
                .member_delivery("workers", "member-a")
                .unwrap()
                .is_none()
        );
    }
}
