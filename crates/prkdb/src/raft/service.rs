use crate::raft::node::RaftNode;
use crate::raft::rpc::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// gRPC service implementation for Raft
pub struct RaftServiceImpl {
    node: Arc<RaftNode>,
}

impl RaftServiceImpl {
    pub fn new(node: Arc<RaftNode>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl raft_service_server::RaftService for RaftServiceImpl {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            "Received RequestVote from candidate {} for term {}",
            req.candidate_id,
            req.term
        );

        let (term, vote_granted) = self
            .node
            .handle_request_vote(
                req.term,
                req.candidate_id,
                req.last_log_index,
                req.last_log_term,
            )
            .await;

        tracing::debug!(
            "RequestVote response: term={}, vote_granted={}",
            term,
            vote_granted
        );

        Ok(Response::new(RequestVoteResponse { term, vote_granted }))
    }

    async fn pre_vote(
        &self,
        request: Request<PreVoteRequest>,
    ) -> Result<Response<PreVoteResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            "Received PreVote from candidate {} for term {}",
            req.candidate_id,
            req.term
        );

        let (term, vote_granted) = self
            .node
            .handle_pre_vote(
                req.term,
                req.candidate_id,
                req.last_log_index,
                req.last_log_term,
            )
            .await;

        tracing::debug!(
            "PreVote response: term={}, vote_granted={}",
            term,
            vote_granted
        );

        Ok(Response::new(PreVoteResponse { term, vote_granted }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        tracing::trace!(
            "Received AppendEntries from leader {} for term {} ({} entries)",
            req.leader_id,
            req.term,
            req.entries.len()
        );

        let (term, success) = self
            .node
            .handle_append_entries(
                req.term,
                req.leader_id,
                req.prev_log_index,
                req.prev_log_term,
                req.leader_commit,
                req.entries,
            )
            .await;

        Ok(Response::new(AppendEntriesResponse { term, success }))
    }

    async fn read_index(
        &self,
        request: Request<ReadIndexRequest>,
    ) -> Result<Response<ReadIndexResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            "Received ReadIndex from term {} for leader {}",
            req.term,
            req.leader_id
        );

        match self.node.handle_read_index(req.term).await {
            Ok((term, read_index)) => {
                tracing::debug!(
                    "ReadIndex response: term={}, read_index={}",
                    term,
                    read_index
                );
                Ok(Response::new(ReadIndexResponse {
                    term,
                    success: read_index > 0,
                    read_index,
                }))
            }
            Err(_) => {
                // Not leader
                tracing::debug!("ReadIndex failed: not leader");
                Ok(Response::new(ReadIndexResponse {
                    term: 0,
                    success: false,
                    read_index: 0,
                }))
            }
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let (term, _success) = self
            .node
            .handle_install_snapshot(
                req.term,
                req.leader_id,
                req.last_included_index,
                req.last_included_term,
                req.data,
            )
            .await;

        let response = InstallSnapshotResponse { term };
        Ok(Response::new(response))
    }
}
