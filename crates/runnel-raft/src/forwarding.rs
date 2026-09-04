use std::collections::BTreeMap;
use std::time::Duration;

use runnel_engine::{AckResult, BrokerError, Offset, PollResult, ReplayMessage};

use super::NodeId;
use super::group_manager::GroupManager;
use super::network::{self, ForwardedOperation, ForwardedResponse};

const FORWARD_ATTEMPTS: usize = 3;
const FORWARD_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) struct ClientForwarder<'a> {
    manager: &'a GroupManager,
    node_id: NodeId,
    peers: &'a BTreeMap<NodeId, String>,
}

impl<'a> ClientForwarder<'a> {
    pub(super) fn new(
        manager: &'a GroupManager,
        node_id: NodeId,
        peers: &'a BTreeMap<NodeId, String>,
    ) -> Self {
        Self {
            manager,
            node_id,
            peers,
        }
    }

    async fn operation_leader(
        &self,
        operation: &ForwardedOperation,
    ) -> Result<Option<NodeId>, BrokerError> {
        match operation {
            ForwardedOperation::CreateStream { .. } => Ok(self
                .manager
                .metadata_group()
                .await
                .raft()
                .current_leader()
                .await),
            ForwardedOperation::Publish { stream, .. }
            | ForwardedOperation::Poll { stream, .. }
            | ForwardedOperation::Replay { stream, .. }
            | ForwardedOperation::Ack { stream, .. }
            | ForwardedOperation::PollGroup { stream, .. }
            | ForwardedOperation::AckGroup { stream, .. }
            | ForwardedOperation::InitializeDataStream { stream, .. } => Ok(self
                .manager
                .data_group_for_stream(stream)
                .await?
                .raft()
                .current_leader()
                .await),
        }
    }

    async fn operation(
        &self,
        operation: ForwardedOperation,
        mut leader_id: Option<NodeId>,
    ) -> Result<ForwardedResponse, BrokerError> {
        let mut last_error = None;
        for _ in 0..FORWARD_ATTEMPTS {
            let preferred_leader = if let Some(leader_id) = leader_id.take() {
                Some(leader_id)
            } else {
                self.operation_leader(&operation).await?
            };
            let mut candidates = self
                .peers
                .keys()
                .copied()
                .filter(|target| *target != self.node_id)
                .collect::<Vec<_>>();
            if let Some(preferred_leader) =
                preferred_leader.filter(|target| *target != self.node_id)
            {
                candidates.retain(|target| *target != preferred_leader);
                candidates.insert(0, preferred_leader);
            }

            for target in candidates {
                let Some(address) = self.peers.get(&target) else {
                    last_error = Some(format!("leader node {target} has no configured address"));
                    continue;
                };
                let response = match network::forward(
                    self.manager.peer_transport(),
                    address,
                    operation.clone(),
                    FORWARD_TIMEOUT,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        last_error = Some(format!("leader forwarding failed: {error}"));
                        continue;
                    }
                };
                if let Some(next_leader) = forwarded_leader(&response) {
                    if let Some(next_leader) = next_leader {
                        leader_id = Some(next_leader);
                    } else {
                        last_error = Some("peer has no elected leader".to_owned());
                    }
                    continue;
                }
                return Ok(response);
            }
        }
        Err(BrokerError::Cluster(last_error.unwrap_or_else(|| {
            "cluster has no elected leader".to_owned()
        })))
    }

    pub(super) async fn create_stream(
        &self,
        stream: String,
        leader_id: Option<NodeId>,
    ) -> Result<bool, BrokerError> {
        match self
            .operation(ForwardedOperation::CreateStream { stream }, leader_id)
            .await?
        {
            ForwardedResponse::CreateStream(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong create-stream response".to_owned(),
            )),
        }
    }

    pub(super) async fn publish(
        &self,
        operation: ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<Offset, BrokerError> {
        match self.operation(operation, leader_id).await? {
            ForwardedResponse::Publish(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong publish response".to_owned(),
            )),
        }
    }

    pub(super) async fn poll(
        &self,
        stream: String,
        consumer: String,
        leader_id: Option<NodeId>,
    ) -> Result<PollResult, BrokerError> {
        match self
            .operation(ForwardedOperation::Poll { stream, consumer }, leader_id)
            .await?
        {
            ForwardedResponse::Poll(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong poll response".to_owned(),
            )),
        }
    }

    pub(super) async fn replay(
        &self,
        operation: ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<ReplayMessage, BrokerError> {
        match self.operation(operation, leader_id).await? {
            ForwardedResponse::Replay(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong replay response".to_owned(),
            )),
        }
    }

    pub(super) async fn ack(
        &self,
        operation: ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<AckResult, BrokerError> {
        match self.operation(operation, leader_id).await? {
            ForwardedResponse::Ack(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong acknowledgement response".to_owned(),
            )),
        }
    }

    pub(super) async fn poll_group(
        &self,
        stream: String,
        consumer: String,
        member: String,
        leader_id: Option<NodeId>,
    ) -> Result<PollResult, BrokerError> {
        match self
            .operation(
                ForwardedOperation::PollGroup {
                    stream,
                    consumer,
                    member,
                },
                leader_id,
            )
            .await?
        {
            ForwardedResponse::PollGroup(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong grouped poll response".to_owned(),
            )),
        }
    }

    pub(super) async fn ack_group(
        &self,
        operation: ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<AckResult, BrokerError> {
        match self.operation(operation, leader_id).await? {
            ForwardedResponse::AckGroup(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong grouped acknowledgement response".to_owned(),
            )),
        }
    }
}

fn forwarded_leader(response: &ForwardedResponse) -> Option<Option<NodeId>> {
    match response {
        ForwardedResponse::CreateStream(Err(network::ForwardError::NotLeader { leader_id }))
        | ForwardedResponse::Publish(Err(network::ForwardError::NotLeader { leader_id }))
        | ForwardedResponse::Poll(Err(network::ForwardError::NotLeader { leader_id }))
        | ForwardedResponse::Replay(Err(network::ForwardError::NotLeader { leader_id }))
        | ForwardedResponse::Ack(Err(network::ForwardError::NotLeader { leader_id }))
        | ForwardedResponse::PollGroup(Err(network::ForwardError::NotLeader { leader_id }))
        | ForwardedResponse::AckGroup(Err(network::ForwardError::NotLeader { leader_id })) => {
            Some(*leader_id)
        }
        _ => None,
    }
}

pub(super) fn forward_error_to_broker(error: network::ForwardError) -> BrokerError {
    match error {
        network::ForwardError::NotLeader { leader_id } => BrokerError::NotLeader { leader_id },
        network::ForwardError::AckNotInFlight { consumer, offset } => {
            BrokerError::AckNotInFlight { consumer, offset }
        }
        network::ForwardError::StaleDelivery { consumer, offset } => {
            BrokerError::StaleDelivery { consumer, offset }
        }
        network::ForwardError::HistoryUnavailable {
            stream,
            requested_offset,
            earliest_offset,
            next_offset,
        } => BrokerError::HistoryUnavailable {
            stream,
            requested_offset,
            earliest_offset,
            next_offset,
        },
        network::ForwardError::Message(message) => BrokerError::Cluster(message),
    }
}
