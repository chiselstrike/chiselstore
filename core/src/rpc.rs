//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{Consistency, StoreCommand, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use little_raft::message::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry, Query, QueryResults, QueryRow, Void,
    VoteRequest, VoteResponse,
};

/// Node address lookup function.
pub type NodeAddrFn = dyn Fn(usize) -> (&'static str, u16) + Send + Sync;

#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(
        &self,
        addr: S,
    ) -> Result<RpcClient<tonic::transport::Channel>, tonic::transport::Error> {
        let addr = addr.to_string();
        match self.connections.pop() {
            Some(x) => Ok(x),
            None => RpcClient::connect(addr).await,
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(
        &self,
        addr: S,
    ) -> Result<Connection, tonic::transport::Error> {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Ok(Connection {
            conn: pool.connection(addr).await?,
            pool: pool.clone(),
        })
    }

    async fn invalidate<S: ToString>(&self, addr: S) {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        conns.remove(&addr);
    }
}

/// RPC transport.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Arc<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Arc<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }

    /// Node RPC address in cluster.
    fn node_rpc_addr(&self, id: usize) -> String {
        let (host, port) = (*self.node_addr)(id);
        format!("http://{}:{}", host, port)
    }
}

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send(&self, to_id: usize, msg: Message<StoreCommand, Bytes>) {
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
                let peer = self.node_rpc_addr(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    if let Ok(mut client) = pool.connection(&peer).await {
                        let request = tonic::Request::new(request.clone());
                        if client.conn.append_entries(request).await.is_err() {
                            pool.invalidate(peer).await
                        }
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
                let peer = self.node_rpc_addr(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    if let Ok(mut client) = pool.connection(&peer).await {
                        let request = tonic::Request::new(request.clone());
                        if client
                            .conn
                            .respond_to_append_entries(request)
                            .await
                            .is_err()
                        {
                            pool.invalidate(peer).await
                        }
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
                let peer = self.node_rpc_addr(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    if let Ok(mut client) = pool.connection(&peer).await {
                        let vote = tonic::Request::new(request.clone());
                        if client.conn.vote(vote).await.is_err() {
                            pool.invalidate(peer).await
                        }
                    }
                });
            }
            Message::VoteResponse {
                from_id,
                term,
                vote_granted,
            } => {
                let peer = self.node_rpc_addr(to_id);
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
            Message::InstallSnapshotRequest { .. } => {
                todo!("Snapshotting is not implemented.");
            }
            Message::InstallSnapshotResponse { .. } => {
                todo!("Snapshotting is not implemented.");
            }
        }
    }

    async fn delegate(
        &self,
        to_id: usize,
        sql: String,
        consistency: Consistency,
    ) -> Result<crate::server::QueryResults, crate::StoreError> {
        let addr = self.node_rpc_addr(to_id);
        let mut client = self.connections.connection(addr.clone()).await.unwrap();
        let query = tonic::Request::new(Query {
            sql,
            consistency: consistency as i32,
        });
        let response = client.conn.execute(query).await.unwrap();
        let response = response.into_inner();
        let mut rows = vec![];
        for row in response.rows {
            rows.push(crate::server::QueryRow { values: row.values });
        }
        Ok(crate::server::QueryResults { rows })
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
        let consistency =
            proto::Consistency::from_i32(query.consistency).unwrap_or(proto::Consistency::Strong);
        let consistency = match consistency {
            proto::Consistency::Strong => Consistency::Strong,
            proto::Consistency::RelaxedReads => Consistency::RelaxedReads,
        };
        let server = self.server.clone();
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
        let server = self.server.clone();
        server.recv_msg(msg);
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
        let server = self.server.clone();
        server.recv_msg(msg);
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
        let server = self.server.clone();
        server.recv_msg(msg);
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
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }
}
