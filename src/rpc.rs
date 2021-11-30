//! ChiselStore RPC module.

use derivative::Derivative;
use little_raft::message::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::rpc::proto::rpc_server::Rpc;
use crate::{Consistency, StoreCommand, StoreServer, StoreTransport};

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry, Query, QueryResults, QueryRow, Void,
    VoteRequest, VoteResponse,
};

type NodeAddrFn = dyn Fn(usize) -> String + Send;

/// RPC transport.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport { node_addr }
    }
}

impl StoreTransport for RpcTransport {
    fn send(&self, to_id: usize, msg: Message<StoreCommand>) {
        match msg {
            Message::AppendEntryRequest {
                from_id,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                commit_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let prev_log_index = prev_log_index as u64;
                let prev_log_term = prev_log_term as u64;
                let entries = entries
                    .iter()
                    .map(|entry| {
                        let id = entry.transition.id as u64;
                        let index = entry.index as u64;
                        let sql = entry.transition.sql.clone();
                        let term = entry.term as u64;
                        LogEntry {
                            id,
                            sql,
                            index,
                            term,
                        }
                    })
                    .collect();
                let commit_index = commit_index as u64;
                let request = AppendEntriesRequest {
                    from_id,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    commit_index,
                };
                let peer = (self.node_addr)(to_id);
                tokio::task::spawn(async move {
                    let request = request.clone();
                    if let Ok(mut client) = RpcClient::connect(peer.to_string()).await {
                        let request = tonic::Request::new(request.clone());
                        client.append_entries(request).await.unwrap();
                    }
                });
            }
            Message::AppendEntryResponse {
                from_id,
                term,
                success,
                last_index,
                mismatch_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_index = last_index as u64;
                let mismatch_index = mismatch_index.map(|idx| idx as u64);
                let request = AppendEntriesResponse {
                    from_id,
                    term,
                    success,
                    last_index,
                    mismatch_index,
                };
                let peer = (self.node_addr)(to_id);
                tokio::task::spawn(async move {
                    let request = request.clone();
                    if let Ok(mut client) = RpcClient::connect(peer.to_string()).await {
                        let request = tonic::Request::new(request.clone());
                        client.respond_to_append_entries(request).await.unwrap();
                    }
                });
            }
            Message::VoteRequest {
                from_id,
                term,
                last_log_index,
                last_log_term,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_log_index = last_log_index as u64;
                let last_log_term = last_log_term as u64;
                let request = VoteRequest {
                    from_id,
                    term,
                    last_log_index,
                    last_log_term,
                };
                let peer = (self.node_addr)(to_id);
                tokio::task::spawn(async move {
                    let request = request.clone();
                    if let Ok(mut client) = RpcClient::connect(peer.to_string()).await {
                        let vote = tonic::Request::new(request.clone());
                        client.vote(vote).await.unwrap();
                    }
                });
            }
            Message::VoteResponse {
                from_id,
                term,
                vote_granted,
            } => {
                let peer = (self.node_addr)(to_id);
                tokio::task::spawn(async move {
                    let from_id = from_id as u64;
                    let term = term as u64;
                    let response = VoteResponse {
                        from_id,
                        term,
                        vote_granted,
                    };
                    if let Ok(mut client) = RpcClient::connect(peer.to_string()).await {
                        let response = tonic::Request::new(response.clone());
                        client.respond_to_vote(response).await.unwrap();
                    }
                });
            }
        }
    }
}

/// RPC service.
#[derive(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        let query = request.into_inner();
        let server = self.server.clone();
        let consistency =
            proto::Consistency::from_i32(query.consistency).unwrap_or(proto::Consistency::Strong);
        let consistency = match consistency {
            proto::Consistency::Strong => Consistency::Strong,
            proto::Consistency::RelaxedReads => Consistency::RelaxedReads,
        };
        let results = match server.query(query.sql, consistency).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(QueryResults { rows }))
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let last_log_index = msg.last_log_index as usize;
        let last_log_term = msg.last_log_term as usize;
        let msg = little_raft::message::Message::VoteRequest {
            from_id,
            term,
            last_log_index,
            last_log_term,
        };
        self.server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }

    async fn respond_to_vote(
        &self,
        request: Request<VoteResponse>,
    ) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let vote_granted = msg.vote_granted;
        let msg = little_raft::message::Message::VoteResponse {
            from_id,
            term,
            vote_granted,
        };
        self.server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let prev_log_index = msg.prev_log_index as usize;
        let prev_log_term = msg.prev_log_term as usize;
        let entries: Vec<little_raft::message::LogEntry<StoreCommand>> = msg
            .entries
            .iter()
            .map(|entry| {
                let id = entry.id as usize;
                let sql = entry.sql.to_string();
                let transition = StoreCommand { id, sql };
                let index = entry.index as usize;
                let term = entry.term as usize;
                little_raft::message::LogEntry {
                    transition,
                    index,
                    term,
                }
            })
            .collect();
        let commit_index = msg.commit_index as usize;
        let msg = little_raft::message::Message::AppendEntryRequest {
            from_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            commit_index,
        };
        self.server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }

    async fn respond_to_append_entries(
        &self,
        request: tonic::Request<AppendEntriesResponse>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let success = msg.success;
        let last_index = msg.last_index as usize;
        let mismatch_index = msg.mismatch_index.map(|idx| idx as usize);
        let msg = little_raft::message::Message::AppendEntryResponse {
            from_id,
            term,
            success,
            last_index,
            mismatch_index,
        };
        self.server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }
}
