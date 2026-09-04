#![allow(clippy::result_large_err)]

//! OpenRaft integration behind Runnel's topology-free engine contract.
//!
//! The persistent engine uses versioned local files and a framed TCP peer
//! protocol. It is an early clustered backend: membership is static, each
//! stream has a data group, and public requests are routed to the elected
//! leader through the internal peer protocol.

#[cfg(test)]
use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use openraft::BasicNode;
#[cfg(test)]
use openraft::storage::RaftStateMachine;
#[cfg(test)]
use openraft::{Entry, EntryPayload, RaftSnapshotBuilder, SnapshotMeta};
#[cfg(test)]
use openraft::{LogId, StoredMembership};
use runnel_engine::BrokerError;
#[cfg(test)]
use runnel_engine::{AckResult, Engine, Message, Offset, PollResult};

mod delivery;
mod engine;
mod forwarding;
mod group_manager;
mod log_store;
mod network;
mod state_machine;
mod state_machine_journal;
mod state_machine_store;

#[cfg(test)]
use delivery::lease_expired;
pub use engine::{InMemoryCluster, PersistentEngine, RaftGroup, SingleNodeEngine};
#[cfg(test)]
use group_manager::DataGroupManifest;
pub use group_manager::GroupManager;
pub use state_machine::{Command, CommandResponse, StreamLifecycle, StreamMetadata};
#[cfg(test)]
use state_machine::{GroupKind, SnapshotState, StoredMessage, StreamState, apply_command};
#[cfg(test)]
use state_machine_journal::JournalEntry as StateMachineJournalEntry;
#[cfg(test)]
use state_machine_journal::{
    FILE as STATE_MACHINE_JOURNAL_FILE, FORMAT_VERSION as STATE_MACHINE_JOURNAL_FORMAT_VERSION,
    is_log_after, read as read_state_machine_journal,
};
pub use state_machine_store::SnapshotMetricsSnapshot;
#[cfg(test)]
use state_machine_store::StateMachineStore;
#[cfg(test)]
use state_machine_store::{
    PersistedSnapshotState, PersistedSnapshotStateRef, PersistedStreamData, StoredSnapshot,
    snapshot_state_from_persisted, validate_snapshot_data,
};

pub type NodeId = u64;
pub const METADATA_GROUP_ID: &str = "metadata";

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        NodeId = NodeId,
        Node = BasicNode,
);

pub type Raft = openraft::Raft<TypeConfig>;

pub async fn serve_peer(
    listener: tokio::net::TcpListener,
    manager: Arc<GroupManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    network::serve(listener, manager, shutdown).await
}

const FORMAT_VERSION: u32 = 2;

#[cfg(test)]
use engine::{
    PersistedStorageMetadata, STORAGE_METADATA_FILE, STORAGE_METADATA_FORMAT_VERSION, now_ms,
};

fn path_component(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn validate_name(kind: &'static str, name: &str) -> Result<(), BrokerError> {
    let valid_length = (1..=128).contains(&name.len());
    let valid_characters = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid_length && valid_characters {
        return Ok(());
    }
    Err(BrokerError::InvalidName {
        kind,
        name: name.to_owned(),
    })
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = fs::File::create(&temp)?;
    use std::io::Write;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    let directory = fs::File::open(parent)?;
    directory.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn grouped_test_state() -> SnapshotState {
        let mut state = SnapshotState::default();
        state.streams.insert(
            "events".to_owned(),
            StreamState::active("stream/events".to_owned(), "group/events/data".to_owned()),
        );
        state
            .streams
            .get_mut("events")
            .unwrap()
            .messages
            .push(StoredMessage {
                key: None,
                payload: b"lease".to_vec(),
                published_at_ms: 1,
            });
        state.streams.insert(
            "clock".to_owned(),
            StreamState::active("stream/clock".to_owned(), "group/clock/data".to_owned()),
        );
        state
    }

    fn data_group_kind(stream: &str) -> GroupKind {
        GroupKind::Data {
            stream: stream.to_owned(),
            stream_id: format!("stream/{stream}"),
            group_id: format!("group/{stream}/data"),
        }
    }

    struct PollGroupTestRequest<'a> {
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        now_ms: u64,
        lease_deadline_ms: u64,
        max_delivery_attempts: Option<u32>,
    }

    fn poll_group_for_test(
        state: &mut SnapshotState,
        stream: &str,
        consumer: &str,
        member: &str,
        now_ms: u64,
        lease_deadline_ms: u64,
        log_index: u64,
    ) -> PollResult {
        poll_group_with_max_attempts_for_test(
            state,
            PollGroupTestRequest {
                stream,
                consumer,
                member,
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts: None,
            },
            LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: log_index,
            },
        )
    }

    fn poll_group_with_max_attempts_for_test(
        state: &mut SnapshotState,
        request: PollGroupTestRequest<'_>,
        log_id: LogId<NodeId>,
    ) -> PollResult {
        let PollGroupTestRequest {
            stream,
            consumer,
            member,
            now_ms,
            lease_deadline_ms,
            max_delivery_attempts,
        } = request;
        match apply_command(
            state,
            Command::PollGroup {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                member: member.to_owned(),
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts,
            },
            &data_group_kind(stream),
            log_id,
        ) {
            CommandResponse::GroupPoll { result } => result,
            response => panic!("unexpected grouped poll response: {response:?}"),
        }
    }

    fn poll_group_with_log_id_for_test(
        state: &mut SnapshotState,
        stream: &str,
        consumer: &str,
        member: &str,
        now_ms: u64,
        lease_deadline_ms: u64,
        log_id: LogId<NodeId>,
    ) -> PollResult {
        poll_group_with_max_attempts_for_test(
            state,
            PollGroupTestRequest {
                stream,
                consumer,
                member,
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts: None,
            },
            log_id,
        )
    }

    #[test]
    fn user_stream_with_dead_letter_hash_prefix_still_dead_letters() {
        let source = "runnel.dead-letter.user";
        let mut state = SnapshotState::default();
        state.streams.insert(
            source.to_owned(),
            StreamState::active("stream/user".to_owned(), "group/user/data".to_owned()),
        );
        state
            .streams
            .get_mut(source)
            .unwrap()
            .messages
            .push(StoredMessage {
                key: Some("order-1".to_owned()),
                payload: b"poison".to_vec(),
                published_at_ms: 1,
            });

        assert!(matches!(
            poll_group_with_max_attempts_for_test(
                &mut state,
                PollGroupTestRequest {
                    stream: source,
                    consumer: "worker",
                    member: "member-a",
                    now_ms: 0,
                    lease_deadline_ms: 1,
                    max_delivery_attempts: Some(1),
                },
                LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 1,
                },
            ),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        assert_eq!(
            poll_group_with_max_attempts_for_test(
                &mut state,
                PollGroupTestRequest {
                    stream: source,
                    consumer: "worker",
                    member: "member-b",
                    now_ms: 1,
                    lease_deadline_ms: 2,
                    max_delivery_attempts: Some(1),
                },
                LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 2,
                },
            ),
            PollResult::Empty
        );

        let dead_letter = delivery::dead_letter_stream_name(source);
        assert_eq!(dead_letter, format!("{source}.dead-letter"));
        assert_eq!(state.dead_letters, 1);
        assert_eq!(state.streams[&dead_letter].messages.len(), 1);
    }

    #[test]
    fn maximum_length_dead_letter_target_does_not_recurse() {
        let source = "s".repeat(128);
        let mut state = SnapshotState::default();
        state.streams.insert(
            source.to_owned(),
            StreamState::active("stream/long".to_owned(), "group/long/data".to_owned()),
        );
        state
            .streams
            .get_mut(&source)
            .unwrap()
            .messages
            .push(StoredMessage {
                key: None,
                payload: b"poison".to_vec(),
                published_at_ms: 1,
            });

        assert!(matches!(
            poll_group_with_max_attempts_for_test(
                &mut state,
                PollGroupTestRequest {
                    stream: &source,
                    consumer: "worker",
                    member: "member-a",
                    now_ms: 0,
                    lease_deadline_ms: 1,
                    max_delivery_attempts: Some(1),
                },
                LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 1,
                },
            ),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        assert_eq!(
            poll_group_with_max_attempts_for_test(
                &mut state,
                PollGroupTestRequest {
                    stream: &source,
                    consumer: "worker",
                    member: "member-b",
                    now_ms: 1,
                    lease_deadline_ms: 2,
                    max_delivery_attempts: Some(1),
                },
                LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 2,
                },
            ),
            PollResult::Empty
        );

        let dead_letter = delivery::dead_letter_stream_name(&source);
        assert!(dead_letter.starts_with("runnel.dead-letter."));
        assert!(matches!(
            poll_group_with_max_attempts_for_test(
                &mut state,
                PollGroupTestRequest {
                    stream: &dead_letter,
                    consumer: "inspector",
                    member: "member-a",
                    now_ms: 2,
                    lease_deadline_ms: 3,
                    max_delivery_attempts: Some(1),
                },
                LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 3,
                },
            ),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        assert!(matches!(
            poll_group_with_max_attempts_for_test(
                &mut state,
                PollGroupTestRequest {
                    stream: &dead_letter,
                    consumer: "inspector",
                    member: "member-b",
                    now_ms: 3,
                    lease_deadline_ms: 4,
                    max_delivery_attempts: Some(1),
                },
                LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 4,
                },
            ),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        assert!(
            !state
                .streams
                .contains_key(&format!("{dead_letter}.dead-letter"))
        );
    }

    fn round_trip_snapshot_state_for_test(state: &SnapshotState) -> SnapshotState {
        let recovered = serde_json::from_slice::<PersistedSnapshotState>(
            &serde_json::to_vec(&PersistedSnapshotStateRef::new(state)).unwrap(),
        )
        .unwrap();
        snapshot_state_from_persisted(recovered)
    }

    fn delivery_token_for_test(
        state: &mut SnapshotState,
        member: &str,
        now_ms: u64,
        lease_deadline_ms: u64,
        log_index: u64,
    ) -> String {
        match poll_group_for_test(
            state,
            "events",
            "workers",
            member,
            now_ms,
            lease_deadline_ms,
            log_index,
        ) {
            PollResult::Message(message) => message.delivery_token.unwrap(),
            PollResult::Empty => panic!("expected grouped delivery"),
        }
    }

    fn ack_group_for_test(
        state: &mut SnapshotState,
        stream: &str,
        consumer: &str,
        member: &str,
        offset: Offset,
        delivery_token: &str,
        now_ms: u64,
    ) -> CommandResponse {
        apply_command(
            state,
            Command::AckGroup {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                member: member.to_owned(),
                offset,
                delivery_token: delivery_token.to_owned(),
                now_ms,
            },
            &data_group_kind(stream),
            LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 100 + now_ms,
            },
        )
    }

    #[test]
    fn grouped_lease_deadline_boundaries_are_future_past_and_equal() {
        assert!(!lease_expired(200, 199));
        assert!(lease_expired(200, 200));
        assert!(lease_expired(200, 201));

        let mut future_state = grouped_test_state();
        let first_token = delivery_token_for_test(&mut future_state, "member-a", 100, 200, 0);
        let same_delivery = poll_group_for_test(
            &mut future_state,
            "events",
            "workers",
            "member-a",
            199,
            299,
            1,
        );
        assert!(matches!(
            same_delivery,
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                delivery_token: Some(token),
                ..
            }) if token == first_token
        ));

        let mut equal_state = grouped_test_state();
        let first_token = delivery_token_for_test(&mut equal_state, "member-a", 100, 200, 0);
        let redelivery = poll_group_for_test(
            &mut equal_state,
            "events",
            "workers",
            "member-b",
            200,
            300,
            1,
        );
        assert!(matches!(
            redelivery,
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                delivery_token: Some(token),
                ..
            }) if token != first_token
        ));

        let mut past_state = grouped_test_state();
        delivery_token_for_test(&mut past_state, "member-a", 100, 200, 0);
        assert!(matches!(
            poll_group_for_test(
                &mut past_state,
                "events",
                "workers",
                "member-b",
                201,
                301,
                1,
            ),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
    }

    #[test]
    fn grouped_lease_forward_jump_and_fixed_leader_offset_expire_early() {
        let mut forward_jump_state = grouped_test_state();
        let old_token = delivery_token_for_test(&mut forward_jump_state, "member-a", 100, 200, 0);
        let redelivery = poll_group_for_test(
            &mut forward_jump_state,
            "events",
            "workers",
            "member-b",
            10_000,
            10_100,
            1,
        );
        let new_token = match redelivery {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(2));
                message.delivery_token.unwrap()
            }
            PollResult::Empty => panic!("expected redelivery after a forward clock jump"),
        };
        assert_ne!(new_token, old_token);
        assert_eq!(forward_jump_state.lease_clock_ms, 10_000);

        // A successor whose clock is fixed 2 seconds ahead can expire a
        // delivery even when its own new deadline is still in the future.
        let mut offset_state = grouped_test_state();
        let old_token = match poll_group_with_log_id_for_test(
            &mut offset_state,
            "events",
            "workers",
            "member-a",
            1_000,
            2_000,
            LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 0,
            },
        ) {
            PollResult::Message(message) => message.delivery_token.unwrap(),
            PollResult::Empty => panic!("expected initial delivery"),
        };
        let redelivery = poll_group_with_log_id_for_test(
            &mut offset_state,
            "events",
            "workers",
            "member-b",
            3_000,
            4_000,
            LogId {
                leader_id: openraft::CommittedLeaderId::new(2, 2),
                index: 0,
            },
        );
        assert!(!lease_expired(4_000, 3_000));
        match redelivery {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(2));
                assert_ne!(message.delivery_token.as_deref(), Some(old_token.as_str()));
            }
            PollResult::Empty => panic!("expected redelivery after a fixed leader offset"),
        }
        assert_eq!(offset_state.lease_clock_ms, 3_000);
    }

    #[test]
    fn grouped_lease_successor_backward_offset_delays_expiry_until_floor_catches_up() {
        let mut state = grouped_test_state();
        let old_token = match poll_group_with_log_id_for_test(
            &mut state,
            "events",
            "workers",
            "member-a",
            1_000,
            2_000,
            LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 0,
            },
        ) {
            PollResult::Message(message) => message.delivery_token.unwrap(),
            PollResult::Empty => panic!("expected initial delivery"),
        };

        // At the old leader's deadline boundary, a successor whose clock is
        // fixed 500 ms behind reports 1_500. The floor cannot infer elapsed
        // real time, so it must leave the old delivery in flight.
        assert_eq!(
            poll_group_with_log_id_for_test(
                &mut state,
                "events",
                "workers",
                "member-b",
                1_500,
                2_500,
                LogId {
                    leader_id: openraft::CommittedLeaderId::new(2, 2),
                    index: 0,
                },
            ),
            PollResult::Empty
        );
        assert_eq!(state.lease_clock_ms, 1_500);
        let delivery = state
            .group_consumers
            .get(&("events".to_owned(), "workers".to_owned()))
            .and_then(|consumer| consumer.in_flight.get(&0))
            .expect("backward-skewed successor must retain the delivery");
        assert_eq!(delivery.delivery_token, old_token);
        assert_eq!(delivery.deadline_ms, 2_000);

        let new_token = match poll_group_with_log_id_for_test(
            &mut state,
            "events",
            "workers",
            "member-b",
            2_000,
            3_000,
            LogId {
                leader_id: openraft::CommittedLeaderId::new(2, 2),
                index: 1,
            },
        ) {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(2));
                message.delivery_token.unwrap()
            }
            PollResult::Empty => panic!("expected delivery after the floor reaches its deadline"),
        };
        assert_ne!(new_token, old_token);
        assert_eq!(state.lease_clock_ms, 2_000);

        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-a", 0, &old_token, 2_000,
            ),
            CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }
        );
        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-b", 0, &new_token, 2_000,
            ),
            CommandResponse::GroupAcknowledged
        );
    }

    #[test]
    fn grouped_lease_clock_floor_survives_snapshot_recovery_and_backward_time() {
        let mut state = grouped_test_state();
        poll_group_for_test(&mut state, "events", "workers", "member-a", 100, 200, 0);
        assert_eq!(state.lease_clock_ms, 100);
        assert_eq!(
            poll_group_for_test(&mut state, "clock", "workers", "member-a", 200, 300, 1),
            PollResult::Empty
        );
        assert_eq!(state.lease_clock_ms, 200);

        let mut recovered = round_trip_snapshot_state_for_test(&state);
        assert_eq!(recovered.lease_clock_ms, 200);

        assert!(matches!(
            poll_group_for_test(&mut recovered, "events", "workers", "member-b", 150, 250, 2,),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        assert_eq!(recovered.lease_clock_ms, 200);
    }

    #[test]
    fn grouped_lease_preserves_early_expiry_for_deadline_behind_clock_floor() {
        let mut state = grouped_test_state();
        delivery_token_for_test(&mut state, "member-a", 100, 200, 0);

        // A committed observation from another command advances the floor.
        assert_eq!(
            poll_group_for_test(&mut state, "clock", "workers", "member-a", 300, 400, 1),
            PollResult::Empty
        );
        assert_eq!(state.lease_clock_ms, 300);

        // A successor with a regressed clock can submit a deadline behind the
        // floor. Keep that absolute deadline unchanged so it expires on the
        // next command instead of silently extending the delivery.
        let regressed_token = delivery_token_for_test(&mut state, "member-b", 150, 250, 2);
        let delivery = state
            .group_consumers
            .get(&("events".to_owned(), "workers".to_owned()))
            .and_then(|consumer| consumer.in_flight.get(&0))
            .expect("redelivery must be in flight");
        assert_eq!(delivery.deadline_ms, 250);
        assert!(lease_expired(delivery.deadline_ms, state.lease_clock_ms));

        let next_delivery =
            poll_group_for_test(&mut state, "events", "workers", "member-c", 150, 350, 3);
        match next_delivery {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(3));
                assert_ne!(
                    message.delivery_token.as_deref(),
                    Some(regressed_token.as_str())
                );
            }
            PollResult::Empty => panic!("expected the behind-floor deadline to expire"),
        }
    }

    #[test]
    fn grouped_lease_has_no_lazy_expiry_without_a_committed_command() {
        let mut state = grouped_test_state();
        let token = delivery_token_for_test(&mut state, "member-a", 100, 200, 0);

        // This state machine has no timer callback. With no elected leader,
        // no PollGroup/AckGroup command can be committed, so the delivery
        // remains in flight until a later command supplies an observation.
        let recovered = round_trip_snapshot_state_for_test(&state);
        let delivery = recovered
            .group_consumers
            .get(&("events".to_owned(), "workers".to_owned()))
            .and_then(|consumer| consumer.in_flight.get(&0))
            .expect("snapshot recovery must retain the in-flight delivery");
        assert_eq!(delivery.delivery_token, token);
        assert_eq!(recovered.lease_clock_ms, 100);

        let mut recovered = recovered;
        assert!(matches!(
            poll_group_for_test(&mut recovered, "events", "workers", "member-a", 50, 150, 1),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                delivery_token: Some(ref current_token),
                ..
            }) if current_token == &token
        ));
        assert_eq!(recovered.lease_clock_ms, 100);
    }

    #[test]
    fn grouped_ack_preserves_backward_clock_safety_and_fences_expired_tokens() {
        let mut state = grouped_test_state();
        let old_token = delivery_token_for_test(&mut state, "member-a", 100, 200, 0);

        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-a", 0, &old_token, 50,
            ),
            CommandResponse::GroupAcknowledged
        );

        let mut state = grouped_test_state();
        let old_token = delivery_token_for_test(&mut state, "member-a", 100, 200, 0);
        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-a", 0, &old_token, 200,
            ),
            CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }
        );
        let new_token = delivery_token_for_test(&mut state, "member-b", 150, 250, 2);
        assert_ne!(old_token, new_token);
        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-a", 0, &old_token, 150,
            ),
            CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }
        );
        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-b", 0, &new_token, 150,
            ),
            CommandResponse::GroupAcknowledged
        );
    }

    #[tokio::test]
    async fn grouped_lease_survives_journal_restart_and_leader_change() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let store =
            Arc::new(StateMachineStore::open(&state_directory, data_group_kind("events")).unwrap());
        let mut state_machine = store.clone();
        let first_responses = state_machine
            .apply([
                Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1),
                        index: 0,
                    },
                    payload: EntryPayload::Normal(Command::Publish {
                        stream: "events".to_owned(),
                        key: None,
                        payload: b"lease".to_vec(),
                        published_at_ms: 1,
                        request_id: None,
                    }),
                },
                Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1),
                        index: 1,
                    },
                    payload: EntryPayload::Normal(Command::PollGroup {
                        stream: "events".to_owned(),
                        consumer: "workers".to_owned(),
                        member: "member-a".to_owned(),
                        now_ms: 100,
                        lease_deadline_ms: 200,
                        max_delivery_attempts: None,
                    }),
                },
            ])
            .await
            .unwrap();
        let old_token = match &first_responses[1] {
            CommandResponse::GroupPoll {
                result: PollResult::Message(message),
            } => message.delivery_token.clone().unwrap(),
            response => panic!("unexpected initial poll response: {response:?}"),
        };
        drop(state_machine);
        drop(store);

        let reopened =
            StateMachineStore::open(&state_directory, data_group_kind("events")).unwrap();
        {
            let state = reopened.state.read().await;
            assert_eq!(state.state.lease_clock_ms, 100);
            assert_eq!(
                state
                    .state
                    .group_consumers
                    .get(&("events".to_owned(), "workers".to_owned()))
                    .and_then(|consumer| consumer.in_flight.get(&0))
                    .map(|delivery| delivery.delivery_token.as_str()),
                Some(old_token.as_str())
            );
        }

        let reopened = Arc::new(reopened);
        let mut state_machine = reopened.clone();
        let redelivery_responses = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(2, 2),
                    index: 0,
                },
                payload: EntryPayload::Normal(Command::PollGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-b".to_owned(),
                    now_ms: 200,
                    lease_deadline_ms: 300,
                    max_delivery_attempts: None,
                }),
            }))
            .await
            .unwrap();
        let new_token = match &redelivery_responses[0] {
            CommandResponse::GroupPoll {
                result: PollResult::Message(message),
            } => {
                assert_eq!(message.delivery_attempt, Some(2));
                message.delivery_token.clone().unwrap()
            }
            response => panic!("unexpected redelivery response: {response:?}"),
        };
        assert_ne!(new_token, old_token);

        let stale_response = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(2, 2),
                    index: 1,
                },
                payload: EntryPayload::Normal(Command::AckGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-a".to_owned(),
                    offset: 0,
                    delivery_token: old_token,
                    now_ms: 200,
                }),
            }))
            .await
            .unwrap();
        assert_eq!(
            stale_response,
            vec![CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }]
        );

        let acknowledged_response = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(2, 2),
                    index: 2,
                },
                payload: EntryPayload::Normal(Command::AckGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-b".to_owned(),
                    offset: 0,
                    delivery_token: new_token,
                    now_ms: 200,
                }),
            }))
            .await
            .unwrap();
        assert_eq!(
            acknowledged_response,
            vec![CommandResponse::GroupAcknowledged]
        );
    }

    #[tokio::test]
    async fn single_node_raft_commits_and_applies_messages() {
        let engine = SingleNodeEngine::new(1).await.unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        assert_eq!(
            engine
                .publish("events", None, b"hello".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. }) if payload == b"hello"
        ));
        assert_eq!(
            engine.ack("events", "worker", 0).await.unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Empty
        );
    }

    #[tokio::test]
    async fn single_node_raft_implements_shared_delivery_contract() {
        let engine = SingleNodeEngine::new(1).await.unwrap();
        runnel_test_support::assert_publish_batch_contract(&engine).await;
        runnel_test_support::assert_shared_delivery_contract(&engine).await;
    }

    #[tokio::test]
    async fn single_node_raft_implements_replay_contract() {
        let engine = SingleNodeEngine::new(1).await.unwrap();
        runnel_test_support::assert_replay_contract(&engine).await;
    }

    #[tokio::test]
    async fn persistent_raft_implements_shared_delivery_contract() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-group-contract-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        runnel_test_support::assert_publish_batch_contract(&engine).await;
        runnel_test_support::assert_shared_delivery_contract(&engine).await;
        runnel_test_support::assert_independent_consumers_contract(&engine).await;
        runnel_test_support::assert_key_ordering_contract(&engine).await;
    }

    #[tokio::test]
    async fn persistent_raft_implements_replay_contract() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-replay-contract-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();

        runnel_test_support::assert_replay_contract(&engine).await;
    }

    #[tokio::test]
    async fn persistent_raft_replay_and_ordinary_progress_survive_restart_independently() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-replay-restart-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        engine.create_stream("events").await.unwrap();
        engine
            .publish("events", None, b"first".to_vec(), None)
            .await
            .unwrap();
        engine
            .publish("events", None, b"second".to_vec(), None)
            .await
            .unwrap();

        assert_eq!(
            engine.replay("events", "worker", 1).await.unwrap().payload,
            b"second"
        );
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));
        assert_eq!(
            engine.ack("events", "worker", 0).await.unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            engine.replay("events", "worker", 0).await.unwrap().payload,
            b"first"
        );
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-persistent-replay-restart-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            reopened
                .replay("events", "worker", 1)
                .await
                .unwrap()
                .payload,
            b"second"
        );
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 1, .. })
        ));
    }

    #[tokio::test]
    async fn persistent_raft_legacy_consumers_use_clustered_retry_policy() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open_with_config(
            1,
            "runnel-legacy-retry-contract-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
            Duration::from_millis(100),
            Some(2),
        )
        .await
        .unwrap();
        engine.create_stream("events").await.unwrap();
        engine
            .publish(
                "events",
                Some("poison".to_owned()),
                b"dead-letter-me".to_vec(),
                None,
            )
            .await
            .unwrap();

        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(1),
                ..
            })
        ));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(2),
                ..
            })
        ));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Empty
        );
        assert!(matches!(
            engine.poll("events.dead-letter", "inspector").await.unwrap(),
            PollResult::Message(Message {
                offset: 0,
                payload,
                delivery_attempt: Some(1),
                ..
            }) if payload == b"dead-letter-me"
        ));
        assert_eq!(
            engine
                .ack("events.dead-letter", "inspector", 0)
                .await
                .unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(engine.health().await.unwrap().dead_letters, 1);

        drop(engine);
        let reopened = PersistentEngine::open_with_config(
            1,
            "runnel-legacy-retry-contract-test".to_owned(),
            directory.path(),
            peers,
            true,
            Duration::from_millis(100),
            Some(2),
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Empty
        );
        assert_eq!(
            reopened
                .poll("events.dead-letter", "inspector")
                .await
                .unwrap(),
            PollResult::Empty
        );
    }

    #[tokio::test]
    async fn persistent_raft_fences_expired_group_delivery() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let ack_timeout = Duration::from_secs(2);
        let engine = PersistentEngine::open_with_ack_timeout(
            1,
            "runnel-group-expiry-test".to_owned(),
            directory.path(),
            peers,
            true,
            ack_timeout,
        )
        .await
        .unwrap();
        runnel_test_support::assert_expired_delivery_is_fenced(
            &engine,
            ack_timeout + Duration::from_millis(100),
        )
        .await;
    }

    #[tokio::test]
    async fn persistent_raft_recovers_group_delivery_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let ack_timeout = Duration::from_secs(2);
        let engine = PersistentEngine::open_with_ack_timeout(
            1,
            "runnel-group-restart-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
            ack_timeout,
        )
        .await
        .unwrap();
        engine.create_stream("jobs").await.unwrap();
        engine
            .publish("jobs", None, b"recover".to_vec(), None)
            .await
            .unwrap();
        let (old_token, old_attempt) = match engine
            .poll_group("jobs", "workers", "member-a")
            .await
            .unwrap()
        {
            PollResult::Message(message) => (
                message.delivery_token.unwrap(),
                message.delivery_attempt.unwrap(),
            ),
            PollResult::Empty => panic!("expected grouped delivery"),
        };
        assert_eq!(old_attempt, 1);
        drop(engine);

        tokio::time::sleep(ack_timeout + Duration::from_millis(100)).await;
        let reopened = PersistentEngine::open_with_ack_timeout(
            1,
            "runnel-group-restart-test".to_owned(),
            directory.path(),
            peers,
            true,
            ack_timeout,
        )
        .await
        .unwrap();
        let (new_token, new_attempt) = match reopened
            .poll_group("jobs", "workers", "member-b")
            .await
            .unwrap()
        {
            PollResult::Message(message) => (
                message.delivery_token.unwrap(),
                message.delivery_attempt.unwrap(),
            ),
            PollResult::Empty => panic!("expected redelivery after restart"),
        };
        assert_eq!(new_attempt, 2);
        assert_ne!(new_token, old_token);
        assert!(matches!(
            reopened
                .ack_group("jobs", "workers", "member-a", 0, &old_token)
                .await,
            Err(BrokerError::StaleDelivery { .. })
        ));
        assert_eq!(
            reopened
                .ack_group("jobs", "workers", "member-b", 0, &new_token)
                .await
                .unwrap(),
            AckResult::Acknowledged
        );
    }

    #[tokio::test]
    async fn persistent_raft_dead_letters_after_the_configured_attempt_limit() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let ack_timeout = Duration::from_secs(2);
        let engine = PersistentEngine::open_with_config(
            1,
            "runnel-group-dead-letter-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
            ack_timeout,
            Some(2),
        )
        .await
        .unwrap();
        engine.create_stream("events").await.unwrap();
        engine
            .publish(
                "events",
                Some("poison".to_owned()),
                b"do-not-process".to_vec(),
                None,
            )
            .await
            .unwrap();

        let first = engine
            .poll_group("events", "workers", "member-a")
            .await
            .unwrap();
        assert!(matches!(
            first,
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        tokio::time::sleep(ack_timeout + Duration::from_millis(100)).await;
        let second = engine
            .poll_group("events", "workers", "member-b")
            .await
            .unwrap();
        assert!(matches!(
            second,
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        tokio::time::sleep(ack_timeout + Duration::from_millis(100)).await;
        assert_eq!(
            engine
                .poll_group("events", "workers", "member-c")
                .await
                .unwrap(),
            PollResult::Empty
        );

        let dead_letter = engine
            .poll_group("events.dead-letter", "inspector", "member-a")
            .await
            .unwrap();
        let dead_letter_token = match dead_letter {
            PollResult::Message(message) => {
                assert_eq!(message.payload, b"do-not-process");
                assert_eq!(message.key.as_deref(), Some("poison"));
                assert_eq!(message.delivery_attempt, Some(1));
                message.delivery_token.unwrap()
            }
            PollResult::Empty => panic!("expected dead-letter message"),
        };
        assert_eq!(
            engine
                .ack_group(
                    "events.dead-letter",
                    "inspector",
                    "member-a",
                    0,
                    &dead_letter_token
                )
                .await
                .unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(engine.health().await.unwrap().dead_letters, 1);
        drop(engine);

        let reopened = PersistentEngine::open_with_config(
            1,
            "runnel-group-dead-letter-test".to_owned(),
            directory.path(),
            peers,
            true,
            ack_timeout,
            Some(2),
        )
        .await
        .unwrap();
        assert_eq!(
            reopened
                .poll_group("events", "workers", "member-d")
                .await
                .unwrap(),
            PollResult::Empty
        );
        assert_eq!(
            reopened
                .poll_group("events.dead-letter", "inspector", "member-b")
                .await
                .unwrap(),
            PollResult::Empty
        );
        assert_eq!(reopened.health().await.unwrap().dead_letters, 1);
    }

    #[tokio::test]
    async fn three_node_cluster_replicates_a_committed_message() {
        let cluster = InMemoryCluster::new([1, 2, 3]).await.unwrap();
        let leader = cluster.leader().await.unwrap();
        assert!(leader.create_stream("events".to_owned()).await.unwrap());
        assert_eq!(
            leader
                .publish("events".to_owned(), None, b"hello".to_vec(), now_ms(), None)
                .await
                .unwrap(),
            0
        );

        for node_id in [1, 2, 3] {
            let node = cluster.node(node_id).unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if matches!(
                    node.state_machine.poll("events", "worker").await.unwrap(),
                    PollResult::Message(Message { offset: 0, .. })
                ) {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("node {node_id} did not apply the committed message");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    #[tokio::test]
    async fn health_reports_in_flight_deliveries_until_group_acknowledged() {
        let cluster = InMemoryCluster::new([1]).await.unwrap();
        let leader = cluster.leader().await.unwrap();
        assert!(leader.create_stream("events".to_owned()).await.unwrap());
        leader
            .publish(
                "events".to_owned(),
                None,
                b"payload".to_vec(),
                now_ms(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(leader.health().await.in_flight_deliveries, 0);
        let message = match leader
            .poll_group(
                "events".to_owned(),
                "workers".to_owned(),
                "member-a".to_owned(),
            )
            .await
            .unwrap()
        {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected a message"),
        };
        assert_eq!(leader.health().await.in_flight_deliveries, 1);

        leader
            .ack_group(
                "events".to_owned(),
                "workers".to_owned(),
                "member-a".to_owned(),
                message.offset,
                message.delivery_token.expect("group delivery token"),
            )
            .await
            .unwrap();
        assert_eq!(leader.health().await.in_flight_deliveries, 0);
    }

    #[tokio::test]
    async fn persistent_engine_recovers_committed_state_after_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        assert!(!engine.create_stream("events").await.unwrap());
        let metadata = engine.group().stream_metadata("events").await.unwrap();
        assert_eq!(metadata.stream_id, "stream/events");
        assert_eq!(metadata.group_id, "group/events/data");
        assert_eq!(metadata.lifecycle, StreamLifecycle::Active);
        assert_eq!(
            engine
                .publish("events", None, b"durable".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-persistent-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        let reopened_metadata = reopened.group().stream_metadata("events").await.unwrap();
        assert_eq!(reopened_metadata, metadata);
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. }) if payload == b"durable"
        ));
    }

    #[tokio::test]
    async fn persisted_storage_rejects_cluster_identity_mismatch_without_rewriting_data() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-identity-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        assert_eq!(
            engine
                .publish("events", None, b"acknowledged".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        drop(engine);

        let metadata_path = directory.path().join(STORAGE_METADATA_FILE);
        let metadata_before = fs::read(&metadata_path).unwrap();
        let error = match PersistentEngine::open(
            1,
            "another-cluster".to_owned(),
            directory.path(),
            peers.clone(),
            false,
        )
        .await
        {
            Ok(_) => panic!("opening storage under another cluster identity must fail"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("cluster identity mismatch"));
        assert_eq!(fs::read(&metadata_path).unwrap(), metadata_before);

        let reopened = PersistentEngine::open(
            1,
            "runnel-persistent-identity-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. })
                if payload == b"acknowledged"
        ));
    }

    #[tokio::test]
    async fn state_machine_journal_replays_and_discards_a_partial_tail() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let store =
            Arc::new(StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap());
        let mut state_machine = store.clone();
        state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 0,
                },
                payload: EntryPayload::Normal(Command::CreateStream {
                    stream: "events".to_owned(),
                    stream_id: Some("stream/events".to_owned()),
                    group_id: Some("group/events/data".to_owned()),
                }),
            }))
            .await
            .unwrap();
        drop(state_machine);
        drop(store);

        let journal_path = state_directory.join(STATE_MACHINE_JOURNAL_FILE);
        let valid_journal_length = fs::metadata(&journal_path).unwrap().len();
        let mut journal = fs::OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .unwrap();
        journal.write_all(&[0x01, 0x02, 0x03]).unwrap();
        drop(journal);

        let reopened = StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap();
        assert_eq!(
            reopened.metadata("events").await.unwrap(),
            StreamMetadata {
                stream_id: "stream/events".to_owned(),
                group_id: "group/events/data".to_owned(),
                lifecycle: StreamLifecycle::Creating,
            }
        );
        assert_eq!(read_state_machine_journal(&journal_path).unwrap().len(), 1);
        assert_eq!(
            fs::metadata(&journal_path).unwrap().len(),
            valid_journal_length
        );
    }

    #[tokio::test]
    async fn state_machine_journal_replays_a_retained_batch_after_restart() {
        const RETAINED_MESSAGES: u64 = 256;
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let kind = data_group_kind("events");
        let store = Arc::new(StateMachineStore::open(&state_directory, kind.clone()).unwrap());
        let entries = (0..RETAINED_MESSAGES).map(|index| Entry {
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(Command::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: format!("message-{index}").into_bytes(),
                published_at_ms: index,
                request_id: None,
            }),
        });
        let mut state_machine = store.clone();
        let responses = state_machine.apply(entries).await.unwrap();
        assert_eq!(responses.len(), RETAINED_MESSAGES as usize);
        drop(state_machine);
        drop(store);

        let reopened = StateMachineStore::open(&state_directory, kind).unwrap();
        let state = reopened.state.read().await;
        let messages = &state.state.streams.get("events").unwrap().messages;
        assert_eq!(messages.len(), RETAINED_MESSAGES as usize);
        assert!(messages.iter().enumerate().all(|(index, message)| {
            message.payload == format!("message-{index}").into_bytes()
        }));
    }

    #[test]
    fn invalid_persisted_state_machine_is_rejected_with_file_context() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        fs::write(
            state_directory.join("state-machine.json"),
            b"not-a-state-machine",
        )
        .unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid persisted state-machine"));
        assert!(error.contains("state-machine.json"));
    }

    #[tokio::test]
    async fn legacy_cluster_layout_is_rejected_without_creating_new_layout() {
        let directory = tempfile::tempdir().unwrap();
        let legacy_log = directory.path().join("raft-log.json");
        fs::write(&legacy_log, b"legacy acknowledged data").unwrap();
        let legacy_state = directory.path().join("state-machine");
        fs::create_dir_all(&legacy_state).unwrap();
        let legacy_state_marker = legacy_state.join("state-machine.json");
        fs::write(&legacy_state_marker, b"legacy state-machine data").unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-legacy-layout-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("legacy clustered storage must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("legacy single-group storage detected"));
        assert_eq!(fs::read(&legacy_log).unwrap(), b"legacy acknowledged data");
        assert_eq!(
            fs::read(&legacy_state_marker).unwrap(),
            b"legacy state-machine data"
        );
        assert!(!directory.path().join(STORAGE_METADATA_FILE).exists());
        assert!(!directory.path().join("groups").exists());
    }

    #[tokio::test]
    async fn legacy_root_checkpoint_is_rejected_without_creating_new_layout() {
        let directory = tempfile::tempdir().unwrap();
        let legacy_checkpoint = directory.path().join("state-machine.json");
        fs::write(&legacy_checkpoint, b"legacy state-machine data").unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-legacy-root-checkpoint-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("legacy root checkpoint must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("legacy single-group storage detected"));
        assert!(error.contains(legacy_checkpoint.to_str().unwrap()));
        assert_eq!(
            fs::read(&legacy_checkpoint).unwrap(),
            b"legacy state-machine data"
        );
        assert!(!directory.path().join(STORAGE_METADATA_FILE).exists());
        assert!(!directory.path().join("groups").exists());
    }

    #[tokio::test]
    async fn unsupported_storage_metadata_version_is_rejected_before_opening_groups() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join(STORAGE_METADATA_FILE);
        fs::write(
            &metadata_path,
            serde_json::to_vec(&serde_json::json!({
                "version": STORAGE_METADATA_FORMAT_VERSION + 1,
                "cluster_name": "runnel-version-test",
                "node_id": 1,
            }))
            .unwrap(),
        )
        .unwrap();
        let metadata_before = fs::read(&metadata_path).unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-version-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("unsupported storage metadata must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("unsupported storage metadata format version"));
        assert_eq!(fs::read(&metadata_path).unwrap(), metadata_before);
        assert!(!directory.path().join("groups").exists());
    }

    #[tokio::test]
    async fn unsupported_data_group_log_is_rejected_before_opening_new_groups() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join(STORAGE_METADATA_FILE),
            serde_json::to_vec(&PersistedStorageMetadata {
                version: STORAGE_METADATA_FORMAT_VERSION,
                cluster_name: "runnel-data-group-version-test".to_owned(),
                node_id: 1,
            })
            .unwrap(),
        )
        .unwrap();
        let metadata_group_directory = directory.path().join("groups/metadata");
        fs::create_dir_all(&metadata_group_directory).unwrap();
        let data_group_directory = directory
            .path()
            .join("groups/data")
            .join(path_component("events"));
        fs::create_dir_all(&data_group_directory).unwrap();
        let manifest_path = data_group_directory.join("group.json");
        let manifest = DataGroupManifest {
            stream: "events".to_owned(),
            stream_id: "stream/events".to_owned(),
            group_id: "group/events/data".to_owned(),
        };
        let manifest_before = serde_json::to_vec(&manifest).unwrap();
        fs::write(&manifest_path, &manifest_before).unwrap();
        let log_path = data_group_directory.join("raft-log.json");
        let log_before = serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "last_purged_log_id": null,
            "log": {},
            "committed": null,
            "vote": null,
        }))
        .unwrap();
        fs::write(&log_path, &log_before).unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-data-group-version-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("unsupported data-group log must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("unsupported log format version"));
        assert!(error.contains(log_path.to_str().unwrap()));
        assert_eq!(fs::read(&manifest_path).unwrap(), manifest_before);
        assert_eq!(fs::read(&log_path).unwrap(), log_before);
        assert!(metadata_group_directory.exists());
        assert!(!metadata_group_directory.join("raft-log.json").exists());
        assert!(!metadata_group_directory.join("state-machine").exists());
    }

    #[tokio::test]
    async fn partial_cluster_layout_is_rejected_without_opening_as_empty() {
        let directory = tempfile::tempdir().unwrap();
        let cluster_name = "runnel-partial-layout-test";
        let metadata_path = directory.path().join(STORAGE_METADATA_FILE);
        let metadata_before = serde_json::to_vec(&PersistedStorageMetadata {
            version: STORAGE_METADATA_FORMAT_VERSION,
            cluster_name: cluster_name.to_owned(),
            node_id: 1,
        })
        .unwrap();
        fs::write(&metadata_path, &metadata_before).unwrap();

        let data_group_directory = directory
            .path()
            .join("groups/data")
            .join(path_component("events"));
        fs::create_dir_all(&data_group_directory).unwrap();
        let manifest_path = data_group_directory.join("group.json");
        let manifest_before = serde_json::to_vec(&DataGroupManifest {
            stream: "events".to_owned(),
            stream_id: "stream/events".to_owned(),
            group_id: "group/events/data".to_owned(),
        })
        .unwrap();
        fs::write(&manifest_path, &manifest_before).unwrap();
        let log_path = data_group_directory.join("raft-log.json");
        let log_before = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "last_purged_log_id": null,
            "log": {},
            "committed": null,
            "vote": null,
        }))
        .unwrap();
        fs::write(&log_path, &log_before).unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error =
            match PersistentEngine::open(1, cluster_name.to_owned(), directory.path(), peers, true)
                .await
            {
                Ok(_) => panic!("partial clustered storage must be rejected"),
                Err(error) => error.to_string(),
            };
        assert!(error.contains("missing metadata group storage"));
        assert!(error.contains("partial clustered layout"));
        assert_eq!(fs::read(&metadata_path).unwrap(), metadata_before);
        assert_eq!(fs::read(&manifest_path).unwrap(), manifest_before);
        assert_eq!(fs::read(&log_path).unwrap(), log_before);
        assert!(!directory.path().join("groups/metadata").exists());
    }

    #[test]
    fn unsupported_state_machine_checkpoint_is_rejected_without_creating_journal() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let state_path = state_directory.join("state-machine.json");
        let state_before = serde_json::to_vec(&serde_json::json!({
            "version": FORMAT_VERSION + 1,
            "last_applied_log": null,
            "last_membership": serde_json::to_value(
                StoredMembership::<NodeId, BasicNode>::default()
            )
            .unwrap(),
            "streams": {},
            "consumers": [],
        }))
        .unwrap();
        fs::write(&state_path, &state_before).unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported state-machine format version"));
        assert!(error.contains(state_path.to_str().unwrap()));
        assert_eq!(fs::read(&state_path).unwrap(), state_before);
        assert!(!state_directory.join(STATE_MACHINE_JOURNAL_FILE).exists());
    }

    #[test]
    fn unsupported_state_machine_journal_is_rejected_without_truncating() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let journal_path = state_directory.join(STATE_MACHINE_JOURNAL_FILE);
        let record = serde_json::to_vec(&StateMachineJournalEntry {
            version: STATE_MACHINE_JOURNAL_FORMAT_VERSION + 1,
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 0,
            },
            payload: EntryPayload::Blank,
        })
        .unwrap();
        let mut journal_before = (record.len() as u32).to_le_bytes().to_vec();
        journal_before.extend_from_slice(&record);
        fs::write(&journal_path, &journal_before).unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported state-machine journal format version"));
        assert!(error.contains(journal_path.to_str().unwrap()));
        assert_eq!(fs::read(&journal_path).unwrap(), journal_before);
        assert!(!state_directory.join("state-machine.json").exists());
    }

    #[test]
    fn unsupported_snapshot_version_is_rejected_without_creating_journal() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let snapshot_path = state_directory.join("snapshot.json");
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "unsupported".to_owned(),
            },
            data: serde_json::to_vec(&serde_json::json!({
                "version": FORMAT_VERSION + 1,
                "streams": {},
                "consumers": [],
            }))
            .unwrap(),
        };
        let snapshot_before = serde_json::to_vec(&snapshot).unwrap();
        fs::write(&snapshot_path, &snapshot_before).unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported snapshot format version"));
        assert!(error.contains(snapshot_path.to_str().unwrap()));
        assert_eq!(fs::read(&snapshot_path).unwrap(), snapshot_before);
        assert!(!state_directory.join(STATE_MACHINE_JOURNAL_FILE).exists());
    }

    #[tokio::test]
    async fn unmarked_clustered_layout_is_rejected_without_guessing_identity() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("groups").join("acknowledged.data");
        fs::create_dir_all(marker.parent().unwrap()).unwrap();
        fs::write(&marker, b"acknowledged data").unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-unmarked-layout-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("unmarked clustered storage must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("missing storage metadata"));
        assert!(!directory.path().join(STORAGE_METADATA_FILE).exists());
        assert_eq!(fs::read(marker).unwrap(), b"acknowledged data");
    }

    #[tokio::test]
    async fn snapshots_bound_consensus_history_and_recover_state() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-snapshot-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        let data_group = engine
            .manager
            .data_group_for_stream("events")
            .await
            .unwrap();
        for index in 0..40 {
            engine
                .publish(
                    "events",
                    None,
                    format!("message-{index}").into_bytes(),
                    None,
                )
                .await
                .unwrap();
        }
        data_group.trigger_snapshot().await.unwrap();
        let snapshot_metrics = data_group.state_machine.snapshot_metrics();
        assert!(snapshot_metrics.builds_started >= 1);
        assert!(snapshot_metrics.builds_completed >= 1);
        let journal_path = directory
            .path()
            .join("groups/data")
            .join(path_component("events"))
            .join("state-machine/state-machine.log");
        assert!(journal_path.exists());

        let snapshot_path = directory
            .path()
            .join("groups/data")
            .join(path_component("events"))
            .join("state-machine/snapshot.json");
        assert!(snapshot_path.exists());
        let snapshot: StoredSnapshot =
            serde_json::from_slice(&fs::read(&snapshot_path).unwrap()).unwrap();
        let remaining_journal = read_state_machine_journal(&journal_path).unwrap();
        assert!(remaining_journal.iter().all(|entry| {
            snapshot
                .meta
                .last_log_id
                .is_none_or(|last| is_log_after(entry.log_id, last))
        }));
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-snapshot-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. })
                if payload == b"message-0"
        ));
    }

    #[tokio::test]
    async fn retained_history_survives_snapshot_install_and_reopen() {
        const RETAINED_MESSAGES: u64 = 256;
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("source-state-machine");
        let kind = GroupKind::Data {
            stream: "events".to_owned(),
            stream_id: "stream/events".to_owned(),
            group_id: "group/events/data".to_owned(),
        };
        let source = Arc::new(StateMachineStore::open(&state_directory, kind.clone()).unwrap());
        let entries = (0..RETAINED_MESSAGES).map(|index| Entry {
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(Command::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: format!("message-{index}").into_bytes(),
                published_at_ms: index,
                request_id: None,
            }),
        });
        let mut source_machine = source.clone();
        let responses = source_machine.apply(entries).await.unwrap();
        assert_eq!(responses.len(), RETAINED_MESSAGES as usize);

        let mut snapshot_builder = source.clone();
        let snapshot = snapshot_builder.build_snapshot().await.unwrap();
        let snapshot_state =
            validate_snapshot_data(&snapshot.snapshot.clone().into_inner()).unwrap();
        let snapshot_messages = match snapshot_state.streams.get("events").unwrap() {
            PersistedStreamData::Current(stream) => &stream.messages,
            PersistedStreamData::Legacy(_) => panic!("new snapshots must use current streams"),
        };
        assert_eq!(snapshot_messages.len(), RETAINED_MESSAGES as usize);
        assert_eq!(
            snapshot_messages.last().unwrap().payload,
            format!("message-{}", RETAINED_MESSAGES - 1).into_bytes()
        );

        let installed_directory = directory.path().join("installed-state-machine");
        let installed =
            Arc::new(StateMachineStore::open(&installed_directory, kind.clone()).unwrap());
        let snapshot_meta = snapshot.meta.clone();
        let snapshot_data = snapshot.snapshot;
        let mut installed_machine = installed.clone();
        installed_machine
            .install_snapshot(&snapshot_meta, snapshot_data)
            .await
            .unwrap();
        drop(installed_machine);
        drop(installed);

        let reopened_source = StateMachineStore::open(&state_directory, kind.clone()).unwrap();
        let reopened_installed = StateMachineStore::open(&installed_directory, kind).unwrap();
        for store in [&reopened_source, &reopened_installed] {
            let state = store.state.read().await;
            let messages = &state.state.streams.get("events").unwrap().messages;
            assert_eq!(messages.len(), RETAINED_MESSAGES as usize);
            assert_eq!(messages.first().unwrap().payload, b"message-0");
            assert_eq!(
                messages.last().unwrap().payload,
                format!("message-{}", RETAINED_MESSAGES - 1).into_bytes()
            );
        }
    }

    #[test]
    fn invalid_persisted_snapshot_is_rejected_before_startup() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "invalid".to_owned(),
            },
            data: b"not-a-snapshot".to_vec(),
        };
        fs::write(
            state_directory.join("snapshot.json"),
            serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid persisted snapshot"));
    }

    #[tokio::test]
    async fn rejected_snapshot_install_preserves_existing_state() {
        let store = Arc::new(StateMachineStore::default());
        {
            let mut state = store.state.write().await;
            state.state.streams.insert(
                "events".to_owned(),
                StreamState {
                    stream_id: "stream/events".to_owned(),
                    group_id: "group/events/data".to_owned(),
                    lifecycle: StreamLifecycle::Active,
                    messages: vec![StoredMessage {
                        key: None,
                        payload: b"durable".to_vec(),
                        published_at_ms: 1,
                    }],
                },
            );
        }
        let before = store.poll("events", "worker").await.unwrap();
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "truncated".to_owned(),
        };
        let mut state_machine = store.clone();
        let result = state_machine
            .install_snapshot(&meta, Box::new(Cursor::new(b"truncated-snapshot".to_vec())))
            .await;
        assert!(result.is_err());
        assert_eq!(store.poll("events", "worker").await.unwrap(), before);
        let metrics = store.snapshot_metrics();
        assert_eq!(metrics.installs_started, 1);
        assert_eq!(metrics.install_failures, 1);
        assert_eq!(metrics.installs_in_progress, 0);
    }

    #[tokio::test]
    async fn legacy_state_machine_format_recovers_metadata_messages_and_progress() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let legacy = serde_json::json!({
            "version": 1,
            "last_applied_log": null,
            "last_membership": serde_json::to_value(
                StoredMembership::<NodeId, BasicNode>::default()
            )
            .unwrap(),
            "streams": {"events": [
                {"key": null, "payload": [115, 107, 105, 112], "published_at_ms": 1},
                {"key": "key", "payload": [114, 101, 99, 111, 118, 101, 114], "published_at_ms": 2}
            ]},
            "consumers": [{"stream": "events", "consumer": "worker", "offset": 1}]
        });
        fs::write(
            state_directory.join("state-machine.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        let store = StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap();
        assert_eq!(
            store.metadata("events").await.unwrap(),
            StreamMetadata {
                stream_id: "stream/events".to_owned(),
                group_id: "group/events/data".to_owned(),
                lifecycle: StreamLifecycle::Active,
            }
        );
        assert!(matches!(
            store.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 1,
                key: Some(key),
                payload,
                ..
            }) if key == "key" && payload == b"recover"
        ));
    }

    #[tokio::test]
    async fn legacy_snapshot_format_recovers_metadata_messages_and_progress() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let snapshot_path = state_directory.join("snapshot.json");
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: Some(LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 1,
                }),
                last_membership: StoredMembership::default(),
                snapshot_id: "legacy".to_owned(),
            },
            data: serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "streams": {"events": [
                    {"key": null, "payload": [115, 107, 105, 112], "published_at_ms": 1},
                    {"key": "key", "payload": [114, 101, 99, 111, 118, 101, 114], "published_at_ms": 2}
                ]},
                "consumers": [{"stream": "events", "consumer": "worker", "offset": 1}]
            }))
            .unwrap(),
        };
        let snapshot_before = serde_json::to_vec(&snapshot).unwrap();
        fs::write(&snapshot_path, &snapshot_before).unwrap();

        let store = StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap();
        assert_eq!(fs::read(&snapshot_path).unwrap(), snapshot_before);
        assert_eq!(
            store.metadata("events").await.unwrap(),
            StreamMetadata {
                stream_id: "stream/events".to_owned(),
                group_id: "group/events/data".to_owned(),
                lifecycle: StreamLifecycle::Active,
            }
        );
        assert!(matches!(
            store.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 1,
                key: Some(key),
                payload,
                ..
            }) if key == "key" && payload == b"recover"
        ));
    }

    #[test]
    fn stream_creation_has_explicit_metadata_and_data_states() {
        let mut metadata_state = SnapshotState::default();
        assert_eq!(
            apply_command(
                &mut metadata_state,
                Command::CreateStream {
                    stream: "events".to_owned(),
                    stream_id: Some("stream/events".to_owned()),
                    group_id: Some("group/events/data".to_owned()),
                },
                &GroupKind::Metadata,
                LogId::default(),
            ),
            CommandResponse::StreamCreated { created: true }
        );
        assert_eq!(
            metadata_state.streams["events"].lifecycle,
            StreamLifecycle::Creating
        );
        assert_eq!(
            apply_command(
                &mut metadata_state,
                Command::ActivateStream {
                    stream: "events".to_owned(),
                },
                &GroupKind::Metadata,
                LogId::default(),
            ),
            CommandResponse::StreamActivated { activated: true }
        );
        assert_eq!(
            metadata_state.streams["events"].lifecycle,
            StreamLifecycle::Active
        );

        let mut data_state = SnapshotState::default();
        assert_eq!(
            apply_command(
                &mut data_state,
                Command::InitializeDataStream {
                    stream: "events".to_owned(),
                    stream_id: "stream/events".to_owned(),
                    group_id: "group/events/data".to_owned(),
                },
                &GroupKind::Data {
                    stream: "events".to_owned(),
                    stream_id: "stream/events".to_owned(),
                    group_id: "group/events/data".to_owned(),
                },
                LogId::default(),
            ),
            CommandResponse::DataStreamInitialized { initialized: true }
        );
        assert_eq!(
            data_state.streams["events"].lifecycle,
            StreamLifecycle::Active
        );
    }

    #[tokio::test]
    async fn persistent_streams_use_independent_data_groups() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-multi-stream-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();

        assert!(engine.create_stream("events").await.unwrap());
        assert!(engine.create_stream("jobs").await.unwrap());
        let events = engine.group().stream_metadata("events").await.unwrap();
        let jobs = engine.group().stream_metadata("jobs").await.unwrap();
        assert_ne!(events.group_id, jobs.group_id);
        assert!(engine.manager.group(&events.group_id).await.is_some());
        assert!(engine.manager.group(&jobs.group_id).await.is_some());

        assert_eq!(
            engine
                .publish("events", None, b"event".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            engine
                .publish("jobs", None, b"job".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { payload, .. }) if payload == b"event"
        ));
        assert!(matches!(
            engine.poll("jobs", "worker").await.unwrap(),
            PollResult::Message(Message { payload, .. }) if payload == b"job"
        ));
    }

    #[tokio::test]
    async fn stream_creation_resumes_after_restart_from_creating_state() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-lifecycle-recovery-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(
            engine
                .group()
                .create_stream("events".to_owned())
                .await
                .unwrap()
        );
        assert_eq!(
            engine
                .group()
                .stream_metadata("events")
                .await
                .unwrap()
                .lifecycle,
            StreamLifecycle::Creating
        );
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-lifecycle-recovery-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert!(!reopened.create_stream("events").await.unwrap());
        assert_eq!(
            reopened
                .group()
                .stream_metadata("events")
                .await
                .unwrap()
                .lifecycle,
            StreamLifecycle::Active
        );
        assert_eq!(
            reopened
                .publish("events", None, b"recovered".to_vec(), None)
                .await
                .unwrap(),
            0
        );
    }
}
