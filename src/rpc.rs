use little_raft::message::Message;

use crate::{StoreCommand, StoreTransport};

pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{AppendEntriesRequest, AppendEntriesResponse, LogEntry, VoteRequest, VoteResponse};

type NodeAddrFn = dyn Fn(usize) -> String + Send;

pub struct RpcTransport {
    node_addr: Box<NodeAddrFn>,
}

impl RpcTransport {
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
